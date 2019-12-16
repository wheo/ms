using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.IO;
using System.Collections;

using Google.Apis.Auth.OAuth2;
using Google.Apis.Services;
using Google.Apis.Util.Store;
using Google.Apis.YouTube.v3;
using Google.Apis.YouTube.v3.Data;

namespace MBCPLUS_DAEMON
{
    public class YTInfo
    {
        public String cid { get; set; }
        public String movpath { get; set; }
        public String videoid { get; set; }
        public String channelid { get; set; }
        public String playlist_id { get; set; }
        public String title { get; set; }
        public String description { get; set; }
        public String tags { get; set; }
        public String isuse { get; set; }
        public String caption_file { get; set; }
        public String category { get; set; }
        public String start_time { get; set; }
        public String end_time { get; set; }
        public String custom_thumbnail { get; set; }
        public String block_outside_ownership { get; set; }
        public String usage_policy { get; set; }
        public String match_policy { get; set; }
        public String enable_content_id { get; set; }
        public String url { get; set; }
        public String enable_contentid { get; set; }
        public String uploader_name { get; set; }
        public String upload_control_id { get; set; }        
        public String spoken_language { get; set; }
        public String custom_id { get; set; }
        public String target_language { get; set; }
        public Boolean IsMovCompleted { get; set; } = false;
        public Boolean IsImgCompleted { get; set; } = false;
        public String session_id { get; set; }

        private YouTubeService ytService;

        public YTInfo()
        {
            ytService = Auth("mbcplus4.json");
        }
        
        public void authentication(String oAuthJson)
        {
            ytService = Auth(oAuthJson);
        }

        private YouTubeService Auth(String oAuthJson)
        {
            ///> key 값을 통한 접근
            //var service = new YouTubeService(new BaseClientService.Initializer()
            //{
            //    //> 구글 계정관리에서 발급받은 api key 값
            //    ApiKey = "AIzaSyAWdL_BMRLu7LIo5SwAue-pVpWe9yEbhQQ",
            //    ApplicationName = "YouTubeAPITest"
            //});

            ///> OAuth 2.0 을 통한 접근
            UserCredential creds;
            using (var stream = new FileStream(String.Format(@"oauth2\{0}",oAuthJson), FileMode.Open, FileAccess.Read))
            {
                creds = GoogleWebAuthorizationBroker.AuthorizeAsync(
                    GoogleClientSecrets.Load(stream).Secrets,
                    //> 연결하려는 권한 설정
                    new[] { YouTubeService.Scope.YoutubeReadonly, YouTubeService.Scope.YoutubeUpload, YouTubeService.Scope.YoutubeForceSsl },
                        "WMS Youtube API",
                    CancellationToken.None,
                    new FileDataStore("WMS YouTube API")).Result;
                //new FileDataStore("YouTubeAPITest"));
            }

            var service = new YouTubeService(new BaseClientService.Initializer()
            {
                HttpClientInitializer = creds,
                //ApiKey = "AIzaSyAWdL_BMRLu7LIo5SwAue-pVpWe9yEbhQQ",
                ApplicationName = "YouTubeAPITest"
            });

            return service;
        }

        public String MakeYoutubeCSVFile()
        {
            string DirPath = @"csv";

            DirectoryInfo di = new DirectoryInfo(DirPath);

            if (di.Exists != true) Directory.CreateDirectory(DirPath);
            String csvFileName = String.Format(@"{0}\{1}.csv", DirPath, cid);
            String FileName = Path.GetFileName(movpath);
            //String channelName = "UCdQvK5uBfTaitckt_n6BRJA";
            String channelName = channelid;
            String privacy = isuse;

            Dictionary<String, String> ytMeta = new Dictionary<string, string>();

            if (privacy == "reservation")
            {
                privacy = "public";
                ytMeta.Add("start_time", start_time);
            }
            //caption_file 있음 확인할 것
            ytMeta.Add("filename", String.Format("\"{0}\"", FileName));
            ytMeta.Add("channel", String.Format("\"{0}\"", channelName));
            ytMeta.Add("privacy", String.Format("\"{0}\"", privacy));
            ytMeta.Add("title", String.Format("\"{0}\"", title));
            ytMeta.Add("description", String.Format("\"{0}\"", description));
            ytMeta.Add("keywords", String.Format("\"{0}\"", tags));
            //ytMeta.Add("block_outside_ownership", "Yes");
            ytMeta.Add("enable_content_id", "Yes");
            ytMeta.Add("usage_policy", usage_policy);
            ytMeta.Add("match_policy", match_policy);
            ytMeta.Add("category", category);
            ytMeta.Add("spoken_language", spoken_language);
            if (IsImgCompleted)
            {
                ytMeta.Add("custom_thumbnail", custom_thumbnail);
            }
            if (!String.IsNullOrEmpty(custom_id))
            {
                ytMeta.Add("custom_id", custom_id);
            }
            ytMeta.Add("playlist_id", playlist_id);
            //channel ep_video_file   ep_thumbnail_file ep_caption_file sh_custom_id se_number  ep_custom_id add_asset_labels    ep_tms_id ep_number   ep_title ep_description  ep_keywords ep_video_genre  ep_rating ep_playlist_id  ep_original_release_date ep_original_release_medium  ep_actors ep_directors    ep_writers ep_producers    ep_spoken_language ep_subtitled_language   ep_caption_language ep_caption_name ownership enable_content_id   match_policy reference_exclusions    ep_privacy require_paid_subscription   usage_policy ep_ad_types ep_ad_break_times ep_start_time   ep_end_time
            String csvBuff = "";

            using (System.IO.StreamWriter file =
            new System.IO.StreamWriter(csvFileName))
            {
                foreach (KeyValuePair<String, String> kv in ytMeta)
                {
                    csvBuff = csvBuff + kv.Key + ",";
                }
                csvBuff = csvBuff.Substring(0, csvBuff.Length - 1);
                file.WriteLine(csvBuff);
                csvBuff = "";
                foreach (KeyValuePair<String, String> kv in ytMeta)
                {
                    csvBuff = csvBuff + kv.Value + ",";
                }
                csvBuff = csvBuff.Substring(0, csvBuff.Length - 1);
                file.WriteLine(csvBuff);
            }
            return csvFileName;
        }

        public List<Dictionary<String, String>> GetChannelList()
        {
            List<Dictionary<String, String>> list = new List<Dictionary<String, String>>();
            var listRequest = ytService.Channels.List("id, contentDetails, snippet");

            //> 인증된 사용자가 소유한 채널만 반환           
            listRequest.Mine = true;

            var response = listRequest.Execute();
            if (response.Items.Count > 0)
            {
                int nItemCount = response.Items.Count;
                for (int i = 0; i < nItemCount; i++)
                {
                    //Console.WriteLine(response.Items[i].Snippet.Title);
                    //Console.WriteLine(response.Items[i].Id);
                    Dictionary<String, String> map = new Dictionary<string, string>();
                    map.Add("channel", response.Items[i].Snippet.Title);
                    map.Add("id", response.Items[i].Id);
                    list.Add(map);
                }
            }
            else
            {

            }
            return list;
        }

        public List<Dictionary<String, String>> GetPlayList(String channelid)
        {
            List<Dictionary<String, String>> list = new List<Dictionary<String, String>>();            
            var listRequest = ytService.Playlists.List("snippet");
            listRequest.MaxResults = 50;            
            
            //> 유튜브에 생성해놓은 채널의 id. url 뒷부분에서 확인 가능
            //listRequest.ChannelId = "UCYw05tiwrozZfKh6RBwiEKA";
            listRequest.ChannelId = channelid;
            
            while ( true )
            {
                var response = listRequest.Execute();                
                if (response.Items.Count > 0)
                {
                    int nItemCount = response.Items.Count;
                    for (int i = 0; i < nItemCount; i++)
                    {
                        //Console.WriteLine(response.Items[i].Snippet.Title);
                        //Console.WriteLine(response.Items[i].Id);
                        Dictionary<String, String> map = new Dictionary<string, string>();
                        map.Add("name", response.Items[i].Snippet.Title);
                        map.Add("id", response.Items[i].Id);
                        map.Add("channel_name", response.Items[i].Snippet.ChannelTitle);
                        map.Add("channel_id", response.Items[i].Snippet.ChannelId);
                        list.Add(map);
                    }
                }
                if ( response.NextPageToken != null)
                {
                    listRequest.PageToken = response.NextPageToken;
                }
                else if ( response == null)
                {
                    break;
                }
                else
                {
                    break;
                }
            }
            /* 응답
            "PL6IrC4MW0OXp2ypp3I5B6CUEcezUd6ITS"
            "PL6IrC4MW0OXryihhTgfHbpdERRHKk6X-j"
            "PL6IrC4MW0OXoKmOzhUhbn_eHjsAWc39rf"
            "PL6IrC4MW0OXqDgZ9QBfRbZZ8X5V8DYVHb"
            */
            return list;
        }

        public void GetPlayListItems()
        {
            var listRequest = ytService.PlaylistItems.List("id, snippet, contentDetails, status");
            //> playlist에서 원하는 리스트 선택하여 조회
            listRequest.PlaylistId = "PL6IrC4MW0OXqDgZ9QBfRbZZ8X5V8DYVHb";

            var response = listRequest.Execute();
            if (response.Items.Count > 0)
            {
                int nItemCount = response.Items.Count;
                for (int i = 0; i < nItemCount; i++)
                {
                    Console.WriteLine(response.Items[i].Snippet.Title);
                    Console.WriteLine(response.Items[i].Id);
                }
            }
            else
            {

            }
        }

        public void insertPlayListItems(string listId, string videoId)
        {
            var newPlayListItem = new PlaylistItem();
            newPlayListItem.Snippet = new PlaylistItemSnippet();
            //> 등록하고자 하는 Playlist Id
            newPlayListItem.Snippet.PlaylistId = listId;
            newPlayListItem.Snippet.ResourceId = new ResourceId();
            newPlayListItem.Snippet.ResourceId.Kind = "youtube#video";
            //> 추가하고자 하는 비디오 ID
            newPlayListItem.Snippet.ResourceId.VideoId = videoId;
            var listRequest = ytService.PlaylistItems.Insert(newPlayListItem, "id, snippet, contentDetails, status");

            var response = listRequest.Execute();
        }

        public void GetCaptions(string videoId)
        {
            var listRequest = ytService.Captions.List("snippet, id", videoId);

            var response = listRequest.Execute();
            if (response.Items.Count > 0)
            {
                int nItemCount = response.Items.Count;
                for (int i = 0; i < nItemCount; i++)
                {
                    //         Console.WriteLine(response.Items[i].Snippet.Title);
                    Console.WriteLine(response.Items[i].Id);
                }
            }
            else
            {

            }
        }

        public void UpdateVideoInfo(string videoId)
         {
            string videoPart = "id, snippet, contentDetails, fileDetails, liveStreamingDetails, player, processingDetails, recordingDetails, statistics, status, suggestions, topicDetails, Localizations";
            //> 업데이트 하려는 비디오 조회.
            var videoRequest = ytService.Videos.List(videoPart);
            //var videoRequest = ytService.Videos.List("snippet, status");
            //var videoRequest = ytService.Videos.List("snippet");
            videoRequest.Id = videoId;

            var response = videoRequest.Execute();
            if (response.Items.Count > 0)
            {
                //> 새로운 값으로 전환
                var Video = response.Items[0];

                ///> snippet - 동영상의 제목, 설명, 카테고리 등 동영상에 대한 기본 세부정보를 포함하는 객체
                ///> status - 동영상의 업로드, 처리, 개인정보 보호 상태에 대한 정보를 포함
                
                //> 기본정보 - 동영상 제목
                Video.Snippet.Title = "New Title2";
                //> 기본정보 - 동영상 설명
                Video.Snippet.Description = "API Update2";
                //> 기본정보 - 테그 추가
                //      List<String> keywords = new List<String>();
                //      Video.Snippet.Tags = new System.Collections.Generic.List<String>();
                Video.Snippet.Tags.Add("temp4");
                Video.Snippet.Tags.Add("temp2");
                Video.Snippet.Tags.Add("temp3");
                Video.Snippet.DefaultLanguage = "en-US";

                //> 기본정보 - 권한
                //Video.Status.PrivacyStatus = "unlisted";    // 미등록
                Video.Status.PrivacyStatus = "private";     // 비공개
                //Video.Status.PrivacyStatus = "public";      // 공개

                //> 메타정보 추가. 
                // 실제로 사용한다고 하면 해당 비디오의 기존 메타를 확인하여 동일한 언어의 경우 내용을 변경하거나, 없는경우 추가하는 방식으로 처리                
                VideoLocalization addLocal = new VideoLocalization();
                addLocal.Title = "Eng1";
                addLocal.Description = "Engdescrip";
                Video.Localizations.Add("ko", addLocal);                

                //> 고급설정 - 동영상과 연결된 YouTube 동영상카테고리 id
                //Video.Snippet.CategoryId = "22";

                //> 고급설정 - 배포 옵션. 동영상을 다른 웹 사이트에 삽입할 수 있는지 여부
                //Video.Status.Embeddable;

                //> 고급설정 - 라이선스 및 소유권. 동영상의 라이센스. 유효값. creativeCommon, youtube
                //Video.Status.License;

                //> 고급설정- 동영상 통계. 동영상 보기 페이지의 확장 동영상 통계가 모든 사용자에게 공개되지는 여부
                //Video.Status.PublicStatsViewable;

                //> 수익창출               
                //VideoMonetizationDetails vmd = new VideoMonetizationDetails();
                //    Video.MonetizationDetails = vmd;

                var updateRequest = ytService.Videos.Update(Video, videoPart);
                updateRequest.Execute();
            }
            else
            {

            }
        }
        /*
        public void GetVidoeInfo(YouTubeVideo video)
        {
            var videoRequest = ytService.Videos.List("snippet");
            videoRequest.Id = video.id;

            var response = videoRequest.Execute();
            if (response.Items.Count > 0)
            {
                video.title = response.Items[0].Snippet.Title;
                video.description = response.Items[0].Snippet.Description;
                video.publishedDate = response.Items[0].Snippet.PublishedAt.Value;
            }
            else
            {

            }
        }
        */
    }
}

    

