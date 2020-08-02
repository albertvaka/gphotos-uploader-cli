package cmd

import (
	"context"
	"fmt"
	"time"

	gphotos "github.com/gphotosuploader/google-photos-api-client-go/lib-gphotos"
	"github.com/gphotosuploader/googlemirror/api/photoslibrary/v1"
	"github.com/spf13/cobra"
	"golang.org/x/oauth2"

	"github.com/gphotosuploader/gphotos-uploader-cli/app"
	"github.com/gphotosuploader/gphotos-uploader-cli/cmd/flags"
	"github.com/gphotosuploader/gphotos-uploader-cli/config"
	"github.com/gphotosuploader/gphotos-uploader-cli/photos"
	"github.com/gphotosuploader/gphotos-uploader-cli/upload"
	"github.com/gphotosuploader/gphotos-uploader-cli/worker"
)

// PushCmd holds the required data for the push cmd
type PushCmd struct {
	*flags.GlobalFlags

	// command flags
	NumberOfWorkers int
}

// DedupeCmd holds the required data for the push cmd
type DedupeCmd struct {
	*flags.GlobalFlags

	// command flags
	NumberOfWorkers int
}

func NewDedupeCmd(globalFlags *flags.GlobalFlags) *cobra.Command {
	cmd := &DedupeCmd{GlobalFlags: globalFlags}

	pushCmd := &cobra.Command{
		Use:   "dedupe",
		Short: "Dedupe albums",
		Long:  `Dedupe albums.`,
		Args:  cobra.NoArgs,
		RunE:  cmd.Run,
	}

	pushCmd.Flags().IntVar(&cmd.NumberOfWorkers, "workers", 1, "Number of workers")

	return pushCmd
}

func NewPushCmd(globalFlags *flags.GlobalFlags) *cobra.Command {
	cmd := &PushCmd{GlobalFlags: globalFlags}

	pushCmd := &cobra.Command{
		Use:   "push",
		Short: "Push local files to Google Photos service",
		Long:  `Scan configured folders in the configuration and push all new object to Google Photos service.`,
		Args:  cobra.NoArgs,
		RunE:  cmd.Run,
	}

	pushCmd.Flags().IntVar(&cmd.NumberOfWorkers, "workers", 5, "Number of workers")

	return pushCmd
}

func (cmd *PushCmd) Run(cobraCmd *cobra.Command, args []string) error {
	cfg, err := config.LoadConfigAndValidate(cmd.CfgDir)
	if err != nil {
		return fmt.Errorf("please review your configuration or run 'gphotos-uploader-cli init': file=%s, err=%s", cmd.CfgDir, err)
	}

	cli, err := app.Start(cfg)
	if err != nil {
		return err
	}
	defer func() {
		_ = cli.Stop()
	}()

	// get OAuth2 Configuration with our App credentials
	oauth2Config := oauth2.Config{
		ClientID:     cfg.APIAppCredentials.ClientID,
		ClientSecret: cfg.APIAppCredentials.ClientSecret,
		Endpoint:     photos.Endpoint,
		Scopes:       photos.Scopes,
	}

	uploadQueue := worker.NewJobQueue(cmd.NumberOfWorkers, cli.Logger)
	uploadQueue.Start()
	defer uploadQueue.Stop()
	time.Sleep(1 * time.Second) // sleeps to avoid log messages colliding with output.

	// launch all folder upload jobs
	ctx := context.Background()
	var totalItems int
	for _, config := range cfg.Jobs {
		c, err := cli.NewOAuth2Client(ctx, oauth2Config, config.Account)
		if err != nil {
			return err
		}

		photosService, err := gphotos.NewClientWithResumableUploads(c, cli.UploadTracker)
		if err != nil {
			return err
		}

		folder := upload.UploadFolderJob{
			FileTracker: cli.FileTracker,

			SourceFolder:       config.SourceFolder,
			CreateAlbum:        config.MakeAlbums.Enabled,
			CreateAlbumBasedOn: config.MakeAlbums.Use,
			Filter:             upload.NewFilter(config.IncludePatterns, config.ExcludePatterns, config.UploadVideos),
		}

		// get UploadItem{} to be uploaded to Google Photos.
		itemsToUpload, err := folder.ScanFolder(cli.Logger)
		if err != nil {
			cli.Logger.Fatalf("Failed to scan folder %s: %v", config.SourceFolder, err)
		}

		// enqueue files to be uploaded. The workers will receive it via channel.
		cli.Logger.Infof("%d files pending to be uploaded in folder '%s'.", len(itemsToUpload), config.SourceFolder)
		totalItems += len(itemsToUpload)
		for _, i := range itemsToUpload {
			uploadQueue.Submit(&upload.EnqueuedJob{
				Context:       ctx,
				PhotosService: photosService,
				FileTracker:   cli.FileTracker,
				Logger:        cli.Logger,

				Path:            i.Path,
				AlbumName:       i.AlbumName,
				DeleteOnSuccess: config.DeleteAfterUpload,
			})
		}
	}

	// get responses from the enqueued jobs
	var uploadedItems int
	for i := 0; i < totalItems; i++ {
		r := <-uploadQueue.ChanJobResults()

		if r.Err != nil {
			cli.Logger.Failf("Error processing %s", r.ID)
		} else {
			uploadedItems++
			cli.Logger.Debugf("Successfully processing %s", r.ID)
		}
	}

	cli.Logger.Infof("%d processed files: %d successfully, %d with errors", totalItems, uploadedItems, totalItems-uploadedItems)
	return nil
}

func getAllPhotosFromAlbum(photosService *gphotos.Client, albumId string) ([]*photoslibrary.MediaItem, error) {
	photos := make([]*photoslibrary.MediaItem, 0)

	token := ""

get_page:
	search1, err := photosService.MediaItems.Search(&photoslibrary.SearchMediaItemsRequest{
		AlbumId:   albumId,
		PageToken: token,
	}).Do()
	if err != nil {
		return nil, err
	}
	photos = append(photos, search1.MediaItems...)

	if len(search1.NextPageToken) > 0 {
		token = search1.NextPageToken
		//fmt.Println(search1.NextPageToken)
		goto get_page
	}

	return photos, nil
}

func addPhotosToAlbum(photosService *gphotos.Client, albumId string, mediaitems []string) error {
	chunkSize := 50 //max number of items per request
	for i := 0; i < len(mediaitems); i += chunkSize {
		end := i + chunkSize
		if end > len(mediaitems) {
			end = len(mediaitems)
		}
		_, err := photosService.Albums.BatchAddMediaItems(albumId, &photoslibrary.AlbumBatchAddMediaItemsRequest{MediaItemIds: mediaitems[i:end]}).Do()
		if err != nil {
			return err
		}
	}
	//fmt.Println("Added", len(mediaitems), "items")
	return nil
}

// Returns the id of the "good album
func dedupe(photosService *gphotos.Client, id1, id2 string) (string, error) {

	a1, err := photosService.Albums.Get(id1).Do()
	if err != nil {
		return "", err
	}
	a2, err := photosService.Albums.Get(id2).Do()
	if err != nil {
		return "", err
	}

	photos1, err := getAllPhotosFromAlbum(photosService, id1)
	if err != nil {
		return "", err
	}
	photos2, err := getAllPhotosFromAlbum(photosService, id2)
	if err != nil {
		return "", err
	}

	fmt.Printf("Dedupe '%s' (%d) and '%s' (%d)\n", a1.Title, len(photos1), a2.Title, len(photos2))

	var itemsToAdd []*photoslibrary.MediaItem
	var albumToAddTo string
	var albumToDelete string

	// Change the smaller number of photos
	if len(photos1) > len(photos2) {
		albumToAddTo = id1
		itemsToAdd = photos2
		albumToDelete = a2.ProductUrl
	} else {
		albumToAddTo = id2
		itemsToAdd = photos1
		albumToDelete = a1.ProductUrl
	}

	var itemIdsToAdd []string
	for _, x := range itemsToAdd {
		itemIdsToAdd = append(itemIdsToAdd, x.Id)
	}

	err = addPhotosToAlbum(photosService, albumToAddTo, itemIdsToAdd)
	if err != nil {
		return "", err
	}

	fmt.Println("To delete: ", albumToDelete)

	return albumToAddTo, nil
}

func (cmd *DedupeCmd) Run(cobraCmd *cobra.Command, args []string) error {
	cfg, err := config.LoadConfigAndValidate(cmd.CfgDir)
	if err != nil {
		return fmt.Errorf("please review your configuration or run 'gphotos-uploader-cli init': file=%s, err=%s", cmd.CfgDir, err)
	}

	cli, err := app.Start(cfg)
	if err != nil {
		return err
	}
	defer func() {
		_ = cli.Stop()
	}()

	// get OAuth2 Configuration with our App credentials
	oauth2Config := oauth2.Config{
		ClientID:     cfg.APIAppCredentials.ClientID,
		ClientSecret: cfg.APIAppCredentials.ClientSecret,
		Endpoint:     photos.Endpoint,
		Scopes:       photos.Scopes,
	}

	uploadQueue := worker.NewJobQueue(cmd.NumberOfWorkers, cli.Logger)
	uploadQueue.Start()
	defer uploadQueue.Stop()
	time.Sleep(1 * time.Second) // sleeps to avoid log messages colliding with output.

	ctx := context.Background()
	for _, config := range cfg.Jobs {
		c, err := cli.NewOAuth2Client(ctx, oauth2Config, config.Account)
		if err != nil {
			return err
		}

		photosService, err := gphotos.NewClientWithResumableUploads(c, cli.UploadTracker)
		if err != nil {
			return err
		}

		token := ""

		albums := make(map[string]string)

		stop_after_first := false

		parsed := 0

	get_page:
		resp, err := photosService.Albums.List().PageSize(50).PageToken(token).Do()
		if err != nil {
			return err
		}

		for _, album := range resp.Albums {
			if dupe_id, exists := albums[album.Title]; exists {
				album_to_keep, err := dedupe(photosService, dupe_id, album.Id)
				if err != nil {
					return err
				}
				albums[album.Title] = album_to_keep
				if stop_after_first {
					return nil
				}
			} else {
				albums[album.Title] = album.Id
			}
			parsed += 1
			//fmt.Println("Parsed:", parsed)
		}

		token = resp.NextPageToken
		if len(token) > 0 {
			//fmt.Println(token)
			fmt.Println("Parsed", parsed, "albums")
			goto get_page
		}
	}

	return nil
}
