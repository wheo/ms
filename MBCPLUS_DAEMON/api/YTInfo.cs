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
using Google.Apis.YouTubePartner.v1;
using Google.Apis.YouTubePartner.v1.Data;

namespace MBCPLUS_DAEMON
{
    public class YTInfo
    {
        public String cid { get; set; }
        public String session_id { get; set; }
        public String movpath { get; set; }
        public String videoid { get; set; }
        public String channelid { get; set; }
        public String playlist_id { get; set; }
        public String title { get; set; }
        public String description { get; set; }
        public String tags { get; set; }
        public String isuse { get; set; }        
        public String category { get; set; }
        public String start_time { get; set; }
        public String end_time { get; set; }
        public String custom_thumbnail { get; set; }
        public String caption_file { get; set; }
        public String caption_language { get; set; }
        public String ownership { get; set; }
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
        public Boolean IsSrtCompleted { get; set; } = false;        
        public String sh_custom_id { get; set; }
        public String information { get; set; }
        public String tmsid { get; set; }
        public String ep_number { get; set; }
        public String ep_original_release_date { get; set; }
        public String status { get; set; }

        private YouTubeService ytService;
        private YouTubePartnerService ytPartnerService;
        
        private Log log;

        public static string StringToCSVCell(string str)
        {
            bool mustQuote = (str.Contains(",") || str.Contains("\"") || str.Contains("\r") || str.Contains("\n"));
            if (mustQuote)
            {
                StringBuilder sb = new StringBuilder();
                sb.Append("\"");
                foreach (char nextChar in str)
                {
                    sb.Append(nextChar);
                    if (nextChar == '"')
                        sb.Append("\"");
                }
                sb.Append("\"");
                return sb.ToString();
            }

            return str;
        }

        public YTInfo()
        {
            if (log == null)
            {
                log = new Log(this.GetType().Name);
            }
            //ytService = Auth("mbcplus1.json");            
        }
        
        public void authentication(String oAuthJson, String brandAccountName)
        {
            ytService = Auth(oAuthJson, brandAccountName);
        }

        private YouTubeService Auth(String oAuthJson, String brandAccountName)
        {
            ///> key 값을 통한 접근
            //var service = new YouTubeService(new BaseClientService.Initializer()
            //{
            //    //> 구글 계정관리에서 발급받은 api key 값
            //    ApiKey = "AIzaSyAWdL_BMRLu7LIo5SwAue-pVpWe9yEbhQQ",
            //    ApplicationName = "YouTubeAPITest"
            //});

            ///> OAuth 2.0 을 통한 접근
            UserCredential creds = null;
            log.logging(brandAccountName);
            using (var stream = new FileStream(String.Format(@"oauth2\{0}",oAuthJson), FileMode.Open, FileAccess.Read))
            {
                //GoogleWebAuthorizationBroker.ReauthorizeAsync(creds, CancellationToken.None);
                creds = GoogleWebAuthorizationBroker.AuthorizeAsync(
                    GoogleClientSecrets.Load(stream).Secrets,
                    //> 연결하려는 권한 설정
                    new[] { YouTubeService.Scope.YoutubeReadonly, YouTubeService.Scope.YoutubeUpload, YouTubeService.Scope.YoutubeForceSsl },
                        brandAccountName,
                    CancellationToken.None,
                    new FileDataStore(Path.GetFileName(oAuthJson))).Result;
                //new FileDataStore("YouTubeAPITest"));
            }
            String accessToken = creds.Token.AccessToken;
            log.logging(oAuthJson + ", " + brandAccountName + " : " + accessToken);

            var service = new YouTubeService(new BaseClientService.Initializer()
            {
                HttpClientInitializer = creds,
                //ApiKey = "AIzaSyAWdL_BMRLu7LIo5SwAue-pVpWe9yEbhQQ",
                ApplicationName = "YouTube_WMS"
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

            //특수문자 추가 패턴 2018-07-25 " 를 ""로

            // 2018-12-27 <, > 를 유튜브가 인식할 수 있는 문자로 변경

            // 2022-10-5 StringToCSVCell 함수로 대체

            //title = title.Replace("\"", "\"\"");
            title = title.Replace("<", "ᐸ");
            title = title.Replace(">", "ᐳ");
            //description = description.Replace("\"", "\"\"");
            description = description.Replace("<", "ᐸ");
            description = description.Replace(">", "ᐳ");

            if (information == "tv")
            {
                if (privacy == "reservation")
                {
                    privacy = "public";
                    ytMeta.Add("ep_start_time", start_time);
                }
                ytMeta.Add("ep_video_file", String.Format("\"{0}\"", FileName));
                ytMeta.Add("channel", String.Format("\"{0}\"", channelName));
                if (IsImgCompleted)
                {
                    ytMeta.Add("ep_thumbnail_file", String.Format("\"{0}\"", custom_thumbnail));
                }                
                ytMeta.Add("sh_custom_id", String.Format("\"{0}\"", sh_custom_id));
                ytMeta.Add("se_number", String.Format("\"{0}\"", "1"));
                if (!String.IsNullOrEmpty(custom_id))
                {
                    ytMeta.Add("ep_custom_id", String.Format("\"{0}\"", custom_id));
                }
                if (!String.IsNullOrEmpty(tmsid))
                {
                    ytMeta.Add("ep_tms_id", String.Format("\"{0}\"", tmsid));
                }
                if (ep_number.Length > 5)
                {
                    ep_number = ep_number.Substring(0, 4);
                }
                if (IsSrtCompleted)
                {
                    ytMeta.Add("ep_caption_file", caption_file);
                    ytMeta.Add("ep_caption_language", caption_language);
                }
                ytMeta.Add("ep_number", String.Format("\"{0}\"", ep_number));
                ytMeta.Add("ep_title", StringToCSVCell(title));
                ytMeta.Add("ep_description", StringToCSVCell(description));
                ytMeta.Add("ep_video_genre", String.Format("\"{0}\"", category));
                ytMeta.Add("ep_keywords", String.Format("\"{0}\"", tags));
                ytMeta.Add("ep_rating", String.Format("\"{0}\"", "Youtube:L0 S0 N0 D0 V0 F0"));
                ytMeta.Add("ep_playlist_id", String.Format("\"{0}\"", playlist_id));
                ytMeta.Add("ep_original_release_date", String.Format("\"{0}\"", ep_original_release_date));
                ytMeta.Add("ep_original_release_medium", String.Format("\"{0}\"", "Basic TV"));
                ytMeta.Add("ep_spoken_language", String.Format("\"{0}\"", spoken_language));
                ytMeta.Add("ownership", String.Format("\"{0}\"", "JP|CA|US"));
                ytMeta.Add("enable_content_id", String.Format("\"{0}\"", "Yes"));
                ytMeta.Add("usage_policy", usage_policy);
                ytMeta.Add("match_policy", match_policy);
                ytMeta.Add("ep_privacy", String.Format("\"{0}\"", privacy));
            } 
            else if ( information == "reference")
            {
                ytMeta.Add("ep_filename", FileName);
                if (!String.IsNullOrEmpty(custom_id))
                {
                    ytMeta.Add("ep_custom_id", String.Format("\"{0}\"", custom_id));
                }
                if (!String.IsNullOrEmpty(tmsid))
                {
                    ytMeta.Add("ep_tms_id", String.Format("\"{0}\"", tmsid));
                }                
                ytMeta.Add("ep_number", String.Format("\"{0}\"", ep_number));
                ytMeta.Add("ep_title", StringToCSVCell(title));
                ytMeta.Add("ep_description", StringToCSVCell(description));
                ytMeta.Add("ownership", String.Format("\"{0}\"", ownership.Replace(",","|") ));
                ytMeta.Add("match_policy", match_policy);
                ytMeta.Add("se_number", String.Format("\"{0}\"", "1"));
            }
            else // moive, web 일단 동일하게 설정
            {
                if (privacy == "reservation")
                {
                    privacy = "public";
                    ytMeta.Add("start_time", start_time);
                }
                //caption_file 있음 확인할 것
                ytMeta.Add("filename", String.Format("\"{0}\"", FileName));
                ytMeta.Add("channel", String.Format("\"{0}\"", channelName));
                ytMeta.Add("privacy", String.Format("\"{0}\"", privacy));                

                ytMeta.Add("title", StringToCSVCell(title));
                ytMeta.Add("description", StringToCSVCell(description));                
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
                if (IsSrtCompleted)
                {
                    ytMeta.Add("caption_file", caption_file);
                    ytMeta.Add("caption_language", caption_language);
                }
                if (!String.IsNullOrEmpty(custom_id))
                {                    
                    ytMeta.Add("custom_id", String.Format("\"{0}\"", custom_id));
                }
                if (!String.IsNullOrEmpty(playlist_id))
                {
                    ytMeta.Add("playlist_id", playlist_id);
                }
            }
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

        public void Dispose()
        {
            
        }

        public List<Dictionary<String, String>> GetChannelList()
        {
            List<Dictionary<String, String>> list = new List<Dictionary<String, String>>();
            var listRequest = ytService.Channels.List("id, contentDetails, snippet");

            //> 인증된 사용자가 소유한 채널만 반환
            //listRequest.Mine = true;
            listRequest.MaxResults = 50;
            listRequest.ManagedByMe = true;
            listRequest.OnBehalfOfContentOwner = "VZldDw5FksRp8XbLQBwBSA";

            while (true)
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
                        log.logging(response.Items[i].Id + ", name : " + response.Items[i].Snippet.Title);
                        list.Add(map);
                    }
                }

                log.logging("next page token : " + response.NextPageToken);

                if (!string.IsNullOrWhiteSpace(response.NextPageToken))
                {
                    log.logging(string.Format($"response pageinfo ({response.PageInfo.ResultsPerPage}/{response.PageInfo.TotalResults})"));
                    listRequest.PageToken = response.NextPageToken;
                    if (response.Items.Count == response.PageInfo.TotalResults)
                    {
                        break;
                    }
                }
                else if (response == null)
                {
                    break;
                }
                else
                {
                    break;
                }
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
                        log.logging("name : " + response.Items[i].Snippet.Title + ", id : " + response.Items[i].Id + ", channel_name : " + response.Items[i].Snippet.ChannelTitle + ", channel_id : " + response.Items[i].Snippet.ChannelId);
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
        }
        public void DeletePlaylistitem_id(String playlistitem_id)
        {
            var request = ytService.PlaylistItems.Delete(playlistitem_id);
            var response = request.Execute();
        }

        public void DeletePlaylistitem(YTMetaInfo ytMetaInfo)
        {
            String[] old_playlist_ids = ytMetaInfo.old_playlist_id.Split(',');
            String playlistitem_ids;
            foreach (String old_playlist_id in old_playlist_ids)
            {
                playlistitem_ids = GetPlaylistFromVideoid(old_playlist_id, ytMetaInfo.videoid);
                String[] playlistitem_id = playlistitem_ids.Split(',');
                foreach (String itemid in playlistitem_id)
                {
                    DeletePlaylistitem_id(itemid);
                }
            }
        }

        public String GetPlaylistFromVideoid(String playlist_id, String videoid)
        {
            String[] playlistitem_ids = null;
            var nextPageToken = "";
            int k = 0;
            while (nextPageToken != null )
            {
                var request = ytService.PlaylistItems.List("snippet");
                request.PlaylistId = playlist_id;
                request.MaxResults = 50;
                request.PageToken = nextPageToken;
                request.VideoId = videoid;
                var response = request.Execute();
                playlistitem_ids = new String[response.Items.Count];
                foreach(var playlistItem in response.Items)
                {
                    playlistitem_ids[k] = playlistItem.Id;
                    k++;
                }
                nextPageToken = response.NextPageToken;
            }            

            return String.Join(",", playlistitem_ids);
        }

        public String InsertPlayListItems(string listId, string videoId)
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
            return response.Id;
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
                    // Console.WriteLine(response.Items[i].Snippet.Title);
                    Console.WriteLine(response.Items[i].Id);
                }
            }
            else
            {

            }
        }

        public void GetOwnerList(YTMetaInfo ytMetaInfo)
        {
            
        }

        public void GetPolicyList(YTMetaInfo ytMetaInfo)
        {
                        
        }

        public void SetThumbNail(YTMetaInfo ytMetaInfo)
        {
            StringBuilder sb = new StringBuilder();
            sb.Append(@"Z:\");
            sb.Append(ytMetaInfo.thumbnailPath);
            String filePath = sb.ToString();
            filePath = filePath.Replace("/", @"\");
            using (var tStream = new FileStream(filePath, FileMode.Open))
            {
                var tInsertRequest = ytService.Thumbnails.Set(ytMetaInfo.videoid, tStream, "image/jpeg");
                tInsertRequest.ProgressChanged += ProcessChanged;

                var uploadThread = new Thread(() => tInsertRequest.Upload());
                uploadThread.Start();
                uploadThread.Join();
            }
        }

        public void SetCaption(YTMetaInfo ytMetaInfo)
        {
            StringBuilder sb = new StringBuilder();
            sb.Append(@"Z:\");
            sb.Append(ytMetaInfo.captionPath);
            String filePath = sb.ToString();
            filePath = filePath.Replace("/", @"\");            
        }

        private void ProcessChanged(Google.Apis.Upload.IUploadProgress obj)
        {
            log.logging(String.Format("{0} byte sent.", obj.BytesSent));
        }

        private void ResponseReceived(Video obj)
        {
            log.logging(String.Format("Video id {0} 가 업로드 되었습니다.", obj.Id));
        }

        public void DeleteVideo(String videoid)
        {
            var videoDeleteRequest = ytService.Videos.Delete(videoid);
            videoDeleteRequest.Execute();
        }

        private String ConvertCategoryID(String id)
        {
            String cateString = null;
            if (id == "2")
            {
                cateString = "Autos & Vehicles";
            }
            else if ( id == "1")
            {
                cateString = "Film & Animation";
            }
            else if ( id == "10")
            {
                cateString = "Music";
            }
            else if ( id == "15")
            {
                cateString = "Pets & Animals";
            }
            else if ( id == "17")
            {
                cateString = "Sports";
            }
            else if ( id  == "18")
            {
                cateString = "Short Movies";
            }
            else if ( id == "19")
            {
                cateString = "Travel & Events";
            }
            else if ( id == "20")
            {
                cateString = "Gaming";
            }
            else if ( id == "21")
            {
                cateString = "Videoblogging";
            }
            else if ( id  == "22")
            {
                cateString = "People & Blogs";
            }
            else if ( id  == "23")
            {
                cateString = "Comedy";
            }
            else if ( id == "24")
            {
                cateString = "Entertainment";
            }
            else if ( id == "25")
            {
                cateString = "News & Politics";
            }
            else if ( id == "26")
            {
                cateString = "Howto & Style";
            }
            else if ( id == "27")
            {
                cateString = "Education";
            }
            else if ( id == "28")
            {
                cateString = "Science & Technology";
            }
            else if ( id == "29")
            {
                cateString = " - Nonprofits & Activism";
            }
            else if ( id =="30")
            {
                cateString = "Movies";
            }
            else if ( id == "31")
            {
                cateString = "Anime / Animation";
            }
            else if ( id == "32")
            {
                cateString = "Action / Adventure";
            }
            else if ( id == "33")
            {
                cateString = "Classics";
            }
            else if ( id =="34")
            {
                cateString = "Comedy";
            }
            else if ( id == "35")
            {
                cateString = "Documentary";
            }
            else if ( id == "36")
            {
                cateString = "Drama";
            }
            else if ( id == "37" )
            {
                cateString = "Family";
            }
            else if ( id == "38")
            {
                cateString = "Foreign";
            }
            else if ( id  == "39")
            {
                cateString = "Horror";
            }
            else if (id == "40")
            {
                cateString = "Sci - Fi / Fantasy";
            }
            else if ( id == "41")
            {
                cateString = "Thriller";
            }
            else if ( id  == "42")
            {
                cateString = "Shorts";
            }
            else if ( id =="43")
            {
                cateString = "Shows";
            }
            else if ( id == "44")
            {
                cateString = "Trailers";
            }
            return cateString;
        }

        public Boolean Sync_WMS(YTMetaInfo ytMetaInfo)
        {
            var videoRequest = ytService.Videos.List("id, snippet, contentDetails, fileDetails, liveStreamingDetails, player, processingDetails, recordingDetails, statistics, status, suggestions, topicDetails, Localizations");
            videoRequest.Id = ytMetaInfo.videoid;
            var response = videoRequest.Execute();
            log.logging(ytMetaInfo.videoid + " : " + response.Items.Count.ToString());
            if (response.Items.Count > 0)
            { 
                ytMetaInfo.title = response.Items[0].Snippet.Title;
                ytMetaInfo.description = response.Items[0].Snippet.Description;
                //ytMetaInfo.start_time = response.Items[0].Status.PublishAtRaw;
                if (response.Items[0].Status.PublishAt != null)
                {
                    ytMetaInfo.start_time_DateTime = response.Items[0].Status.PublishAt.Value;
                    ytMetaInfo.start_time = ytMetaInfo.start_time_DateTime.ToString("yyyy-MM-dd hh:mm:ss");
                }
                else
                {
                    ytMetaInfo.start_time = "";
                }
                ytMetaInfo.privacy = response.Items[0].Status.PrivacyStatus;

                if ( response.Items[0].Snippet.Tags != null)
                {
                    ytMetaInfo.tag = String.Join(",", response.Items[0].Snippet.Tags.ToArray());
                }
                ytMetaInfo.category = ConvertCategoryID(response.Items[0].Snippet.CategoryId);
                ytMetaInfo.spoken_language = response.Items[0].Snippet.DefaultLanguage;
                if (response.Items[0].Snippet.DefaultLanguage == "ko" && response.Items[0].Localizations.ContainsKey("en-US"))
                {
                    ytMetaInfo.target_language = "en-US";
                    ytMetaInfo.trans_lang_title = response.Items[0].Localizations["en-US"].Title;
                    ytMetaInfo.trans_lang_desc = response.Items[0].Localizations["en-US"].Description;                    
                }
                else if (response.Items[0].Snippet.DefaultLanguage == "en-US" && response.Items[0].Localizations.ContainsKey("ko"))
                {
                    ytMetaInfo.target_language = "ko";
                    ytMetaInfo.trans_lang_title = response.Items[0].Localizations["ko"].Title;
                    ytMetaInfo.trans_lang_desc = response.Items[0].Localizations["ko"].Description;
                }
                return true;
            }
            else
            {
                return false;
            }
        }

        public void UpdateVideoInfo(YTMetaInfo ytMetaInfo)
        {
            string videoPart = "id, snippet, contentDetails, fileDetails, liveStreamingDetails, player, processingDetails, recordingDetails, statistics, status, suggestions, topicDetails, Localizations";
            //> 업데이트 하려는 비디오 조회.
            var videoRequest = ytService.Videos.List(videoPart);            
            videoRequest.Id = ytMetaInfo.videoid;

            var response = videoRequest.Execute();
            if (response.Items.Count > 0)
            {
                //> 새로운 값으로 전환
                var video = response.Items[0];                

                ///> snippet - 동영상의 제목, 설명, 카테고리 등 동영상에 대한 기본 세부정보를 포함하는 객체
                ///> status - 동영상의 업로드, 처리, 개인정보 보호 상태에 대한 정보를 포함

                //> 기본정보 - 동영상 제목
                video.Snippet.Title = ytMetaInfo.title;
                //> 기본정보 - 동영상 설명
                video.Snippet.Description = ytMetaInfo.description;
                //> 기본정보 - 테그 추가
                //      List<String> keywords = new List<String>();
                //      Video.Snippet.Tags = new System.Collections.Generic.List<String>();
                //Video.Snippet.Tags.Add("temp4");
                //Video.Snippet.Tags.Add("temp2");
                //Video.Snippet.Tags.Add("temp3");
                if (video.Snippet.Tags != null)
                {
                    video.Snippet.Tags.Clear();
                }
                video.Snippet.Tags = ytMetaInfo.tag.Split(',').ToList();

                if (!String.IsNullOrEmpty(ytMetaInfo.start_time))
                {
                    video.Status.PublishAtRaw = ytMetaInfo.start_time;
                }

                if (String.Equals(ytMetaInfo.privacy, "reservation"))
                {
                    ytMetaInfo.privacy = "private";
                }
                else
                {
                    //ytMetaInfo.privacy = "public";
                    video.Status.PublishAt = null;
                }
                
                //> 기본정보 - 권한
                //Video.Status.PrivacyStatus = "unlisted";    // 미등록
                //Video.Status.PrivacyStatus = "private";     // 비공개
                //Video.Status.PrivacyStatus = "public";      // 공개
                
                video.Status.PrivacyStatus = ytMetaInfo.privacy;
                //> 메타정보 추가. 
                // 실제로 사용한다고 하면 해당 비디오의 기존 메타를 확인하여 동일한 언어의 경우 내용을 변경하거나, 없는경우 추가하는 방식으로 처리                                
                if (video.Localizations != null)
                {
                    video.Localizations.Clear();
                }

                video.Snippet.DefaultLanguage = ytMetaInfo.spoken_language;

                if (ytMetaInfo.spoken_language != ytMetaInfo.target_language)
                {
                    VideoLocalization orgLocal = new VideoLocalization();
                    orgLocal.Title = ytMetaInfo.title;
                    orgLocal.Description = ytMetaInfo.description;
                    video.Localizations.Add(ytMetaInfo.spoken_language, orgLocal);

                    VideoLocalization TransLocal = new VideoLocalization();
                    TransLocal.Title = ytMetaInfo.trans_lang_title;
                    TransLocal.Description = ytMetaInfo.trans_lang_desc;
                    video.Localizations.Add(ytMetaInfo.target_language, TransLocal);                    
                }            

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

                var updateRequest = ytService.Videos.Update(video, videoPart);
                updateRequest.Execute();
            }            
        }

        public void UpdatePlaylistitem(YTMetaInfo ytMetaInfo)
        {
            DeletePlaylistitem(ytMetaInfo);
            String[] playlist_id = ytMetaInfo.playlist_id.Split(',');
            String strPlaylistitem_id = null;
            
            foreach (String playlist in playlist_id)
            {
                strPlaylistitem_id = InsertPlayListItems(playlist, ytMetaInfo.videoid);
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

    

