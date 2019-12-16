using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Net;
using System.IO;
using System.Threading;
using System.Collections.Specialized;
using Newtonsoft.Json.Linq;

namespace MBCPLUS_DAEMON
{
    public class DMInfo
    {
        public String cid {get;set;}
        public String videoid {get;set;}
        public String title { get; set; }        
        public String description { get; set; }
        public String category { get; set; }
        public String tags { get; set; }
        public String isuse { get; set; }
        public String url { get; set; }
        public String playlistid { get; set; }
        public String publish_date { get; set; }
        public String expiry_date { get; set; }
        public String policy_YN { get; set; }
        public String thumbnailURL { get; set; }
        public String geoblock_code { get; set; }
        public String geoblock_value { get; set; }
        public String thumbnail_url { get; set; }
        public String explicit_YN { get; set; }

        private Log log = null;
        SqlMapper mapper = null;

        public DMInfo()
        {
            log = new Log(this.GetType().Name);
            mapper = new SqlMapper();
        }

        public String GetThumbnailURL()
        {
            String url = null;
            url = mapper.GetThumbnail(cid);
            int count = 0;
            while (count < 10)
            {
                if (!String.IsNullOrEmpty(url))
                {
                    break;
                }
                Thread.Sleep(1000);
                log.logging(String.Format("Waiting For Thumbnail ({0})", cid));
                count++;
            }
            return url;
        }

        public static String getPlaylist()
        {
            String url = "https://api.dailymotion.com/playlists?owner=x1u0ca6&page=1&limit=100";
            String response = null;
            response = Http.Get(url);
            return response;
        }

        public static String getChannelList()
        {
            String url = "https://api.dailymotion.com/channels";
            String response = null;
            response = Http.Get(url);
            return response;
        }

        public String GetDmVideoUrl(String url, String accessToken)
        {
            int timeout = (int)5000;

            String responseString = null;
            
            HttpWebRequest request = (HttpWebRequest)WebRequest.Create(url);
            request.Timeout = timeout;
            request.Headers.Add("Authorization", "Bearer " + accessToken);
            using (HttpWebResponse response = (HttpWebResponse)request.GetResponse())
            {
                using (Stream dataStream = response.GetResponseStream())
                {
                    using (StreamReader reader = new StreamReader(dataStream, Encoding.UTF8))
                    {
                        responseString = reader.ReadToEnd();
                    }
                }
            }            
            return responseString;
        }

        public String SetDMPlaylist(String url, String accessToken)
        {
            int timeout = (int)5000;
            String responseString = null;
            try
            {
                HttpWebRequest request = (HttpWebRequest)WebRequest.Create(url);
                request.Timeout = timeout;
                request.Method = "POST";
                request.Headers.Add("Authorization", "Bearer " + accessToken);
                using (HttpWebResponse response = (HttpWebResponse)request.GetResponse())
                {
                    using (Stream dataStream = response.GetResponseStream())
                    {
                        using (StreamReader reader = new StreamReader(dataStream, Encoding.UTF8))
                        {
                            responseString = reader.ReadToEnd();
                        }
                    }
                }
            }
            catch (Exception e)
            {
                log.logging(e.ToString());
            }
            return responseString;
        }

        public String DmVideoUpload(String upload_url, String progress_url, string fullFilePath, String pk)
        {
            FileInfo fi = new FileInfo(fullFilePath);
            FileStream fs = new FileStream(fullFilePath, FileMode.Open, FileAccess.Read);
            String responseString = null;
            String strStatus = null;
            JObject obj;
            double received = 0;
            double size = 0;
            double percent = 0;
            using (fs)
            {
                byte[] buffer = new byte[1024 * 1024 * 8];
                int read = 0;
                long totalRead = 0;
                String sessionID = DateTime.Now.Ticks.ToString("x");
                while (totalRead < fi.Length)
                {
                    read = fs.Read(buffer, 0, buffer.Length);
                    totalRead += read;

                    WebRequest request = WebRequest.Create(upload_url);
                    request.Method = "POST";

                    request.Headers.Add("Content-Disposition", String.Format("attachment; filename=\"{0}\"", Path.GetFileName(fullFilePath)));
                    //request.Headers.Add("Content-Type", "application/octet-stream");
                    request.Headers.Add("X-Content-Range", String.Format("bytes {0}-{1}/{2}", totalRead - read, totalRead - 1, fi.Length));
                    request.Headers.Add("Session-ID", sessionID);
                    request.ContentType = "application/octet-stream";
                    request.ContentLength = read;
                    //log.logging(request.Headers.ToString());
                    Stream dataStream = request.GetRequestStream();
                    dataStream.Write(buffer, 0, read);
                    dataStream.Close();

                    WebResponse response = request.GetResponse();
                    responseString = ((HttpWebResponse)response).StatusDescription;
                    //log.logging(responseString);
                    //log.logging(response.Headers.ToString());
                    dataStream = response.GetResponseStream();
                    StreamReader reader = new StreamReader(dataStream);
                    responseString = reader.ReadToEnd();
                    reader.Close();
                    dataStream.Close();
                    response.Close();
                    // status update
                    strStatus = Http.Get(progress_url);
                    obj = JObject.Parse(strStatus);
                    try
                    {
                        received = (double)obj["received"];
                        size = (double)obj["size"];
                        percent = received / size * 100;
                    }
                    catch 
                    {
                        if ((String)obj["state"] == "done")
                        {
                            percent = 100;
                        }
                    }
                    finally
                    {
                        mapper.UpdateSendingProgress(pk, percent);
                    }
                }
            }
            return responseString;
        }

        public String GetDmRefreshToken(string uri, NameValueCollection pairs)
        {
            byte[] response = null;
            using (WebClient client = new WebClient())
            {
                client.Headers.Add("Content-Type", "application/x-www-form-urlencoded");
                response = client.UploadValues(uri, pairs);
            }
            String responseString = Encoding.UTF8.GetString(response);
            JObject obj;
            obj = JObject.Parse(responseString);
            String access_token = (String)obj["access_token"];
            return access_token;
        }

        public String DmCreateVideo(String url, String videourl, String accessToken)
        {
            byte[] response = null;
            NameValueCollection pairs = new NameValueCollection();
            pairs.Add("url", videourl);
            //pairs.Add("title", "title");
            //pairs.Add("tags", "Tags");

            using (WebClient client = new WebClient())
            {
                client.Headers.Add("Authorization", "Bearer " + accessToken);
                response = client.UploadValues(url, pairs);
            }
            return Encoding.UTF8.GetString(response);
        }

        public String DmPublishVideo(String url, String accessToken)
        {
            byte[] response = null;
            NameValueCollection pairs = new NameValueCollection();

            String[] tag = tags.Split(',');
            //String tags = dmInfo.tags;
            if (tag.Length > 10)
            {
                tags = null;
                List<String> list = new List<String>(tag);
                list.RemoveRange(10, tag.Length-10);
                for (int i = 0; i < list.Count; i++)
                {
                    if (i == 0)
                    {
                        tags = list[i].Trim();
                    }
                    else
                    {
                        tags = tags + "," + list[i].Trim();
                    }
                }
            }
            if (!String.IsNullOrEmpty(tags))
            {
                if (String.Equals(tags[tags.Length - 1], ','))
                {
                    tags = tags.Substring(0, tags.Length - 1);
                }
            }

            String thumbNailUrl = GetThumbnailURL();

            // published 연동할 것
            pairs.Add("private", isuse == "N" ? "true" : "false");
            pairs.Add("published", "false");
            pairs.Add("title", title);
            pairs.Add("tags", tags);
            pairs.Add("channel", category);
            pairs.Add("description", description);
            pairs.Add("publish_date", publish_date);
            pairs.Add("expiry_date", expiry_date);
            if (!String.IsNullOrEmpty(thumbNailUrl))
            {
                pairs.Add("thumbnail_url", thumbNailUrl);
            }
            String geoblockString = String.Format("{0},{1}", geoblock_value, geoblock_code.ToLower());
            if (!String.Equals(geoblock_value, "none") || !String.IsNullOrEmpty(geoblock_value))
            {
                pairs.Add("geoblocking", geoblockString);
            }
            using (WebClient client = new WebClient())
            {
                client.Headers.Add("Authorization", "Bearer " + accessToken);
                response = client.UploadValues(url, pairs);
            }
            return Encoding.UTF8.GetString(response);
        }

        public String DmEditVideo(String url, String accessToken)
        {
            byte[] response = null;
            NameValueCollection pairs = new NameValueCollection();

            String[] tag = tags.Split(',');
            //String tags = dmInfo.tags;
            if (tag.Length > 10)
            {
                tags = null;
                List<String> list = new List<String>(tag);                
                list.RemoveRange(10, tag.Length - 10);
                for (int i = 0; i < list.Count; i++)
                {
                    if (i == 0)
                    {
                        tags = list[i];
                    }
                    else
                    {
                        tags = tags + "," + list[i];
                    }
                }
            }
            if (!String.IsNullOrEmpty(tags))
            {
                if (String.Equals(tags[tags.Length - 1], ','))
                {
                    tags = tags.Substring(0, tags.Length - 1);
                }
            }
            log.logging("tags : " + tags);

            // String thumbNailUrl = dmInfo.GetThumbnailURL();

            if (String.IsNullOrEmpty(category) )
            {
                category = "tv";
            }

            // published 연동할 것
            pairs.Add("id", videoid);
            pairs.Add("private", isuse == "N" ? "true" : "false");
            pairs.Add("published", "false");
            pairs.Add("title", title);
            pairs.Add("tags", tags);
            pairs.Add("channel", category);
            pairs.Add("description", description);
            pairs.Add("publish_date", publish_date);
            pairs.Add("expiry_date", expiry_date);
            //pairs.Add("thumbnail_url", thumbNailUrl);
            String geoblockString = String.Format("{0},{1}", geoblock_value, geoblock_code.ToLower());
            if (!String.Equals(geoblock_value, "none") || !String.IsNullOrEmpty(geoblock_value))
            {
                pairs.Add("geoblocking", geoblockString);
            }
            using (WebClient client = new WebClient())
            {
                client.Headers.Add("Authorization", "Bearer " + accessToken);
                response = client.UploadValues(url, pairs);
            }
            return Encoding.UTF8.GetString(response);
        }

        public Boolean DeleteVideo(String videoid, out String responseString)
        {
            
            if (String.IsNullOrEmpty(Singleton.getInstance().dm_accesstoken))
            {
                NameValueCollection paris = new NameValueCollection();
                String url_refreshtoken = "https://api.dailymotion.com/oauth/token";
                paris.Add("grant_type", "refresh_token");                
                String refreshtoken = Singleton.getInstance().dm_refreshtoken;
                String client_id = Singleton.getInstance().dm_client_id;
                String client_secret = Singleton.getInstance().dm_client_secret;
                Singleton.getInstance().dm_accesstoken = GetDmRefreshToken(url_refreshtoken, paris);
            }

            responseString = null;
            String url = String.Format("https://api.dailymotion.com/video/{0}", videoid);
            int timeout = (int)5000;
            try
            {
                HttpWebRequest request = (HttpWebRequest)WebRequest.Create(url);
                request.Timeout = timeout;
                request.Method = "DELETE";
                request.Headers.Add("Authorization", "Bearer " + Singleton.getInstance().dm_accesstoken);
                using (HttpWebResponse response = (HttpWebResponse)request.GetResponse())
                {
                    using (Stream dataStream = response.GetResponseStream())
                    {
                        using (StreamReader reader = new StreamReader(dataStream, Encoding.UTF8))
                        {
                            responseString = reader.ReadToEnd();
                        }
                    }
                }
            }
            catch (Exception e)
            {
                log.logging(e.ToString());
                return false;
            }
            return true;
        }        
    }
}
