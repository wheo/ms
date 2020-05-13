using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading;
using System.Windows.Forms;
using System.Data;
using System.IO;
using System.Xml;
using MySql.Data;
using MySql.Data.MySqlClient;
using Newtonsoft.Json.Linq;
using WinSCP;
using System.Collections.Specialized;
using System.Net;

namespace MBCPLUS_DAEMON
{
    class DailmotionService
    {
        private Boolean _shouldStop = false;
        private Log log;
        SqlMapper mapper;

        public DailmotionService()
        {
            log = new Log(this.GetType().Name);
            mapper = new SqlMapper();
            DoWork();
        }

        void PlaylistUpdate()
        {
            SqlMapper mapper = new SqlMapper();

            JObject obj = null;
            String response = null;
            try
            {
                response = DMInfo.getPlaylist();
                obj = JObject.Parse(response);
                JArray items = (JArray)obj["list"];
                for (int i = 0; i < items.Count; i++)
                {
                    if (mapper.SetDMPlayList(items[i]["id"].ToString(), items[i]["name"].ToString(), i))
                    {

                    }
                    else
                    {
                        log.logging("SetDMPlayList Failed : " + items[i]["id"].ToString());
                    }
                }
                String responseString = null;

                responseString = DMInfo.getChannelList();

                // 성공시 Completed
                obj = null;
                obj = JObject.Parse(responseString);
                JArray jarray = (JArray)obj["list"];
                DMChannelList dmlist = new DMChannelList();

                foreach (JObject o in jarray)
                {
                    dmlist.id = o["id"].ToString();
                    dmlist.name = o["name"].ToString();
                    dmlist.description = o["description"].ToString();
                    mapper.InsertDMChannelList(dmlist);
                }
            }
            catch (Exception e)
            {
                log.logging(e.ToString());
            }
        }

        void DoWork()
        {
            Thread t1 = new Thread(new ThreadStart(Run));
            t1.Start();
            Thread t2 = new Thread(new ThreadStart(SendToFTP));
            t2.Start();

            Thread.Sleep(5000);
            try
            {
                PlaylistUpdate();
            }
            catch(Exception e)
            {
                log.logging(e.ToString());
            }
        }

        public void RequestStop()
        {
            _shouldStop = true;
        }

        void SendToFTP()
        {
            //Send Ftp Thread
            SqlMapper mapper;
            mapper = new SqlMapper();
            Thread.Sleep(5000);
            vo.DailymotionContentInfo dailymotionContentInfo = new vo.DailymotionContentInfo();

            while (!_shouldStop)
            {
                DataSet ds = new DataSet();
                try
                {
                    if (mapper.DailymotionPendingCheck(ds))
                    {
                        foreach (DataRow r in ds.Tables[0].Rows)
                        {
                            dailymotionContentInfo.videoid = r["videoid"].ToString();
                            dailymotionContentInfo.cid = r["cid"].ToString();
                            dailymotionContentInfo.srcImg = r["srcimg"].ToString();
                            dailymotionContentInfo.srcSubtitle = r["srcsubtitle"].ToString();
                            dailymotionContentInfo.srcMovie = r["srcmov"].ToString();
                            //FTP 등록
                            if( !mapper.PutArchiveToFtp(dailymotionContentInfo) )
                            {
                                //실패
                                log.logging(String.Format("dailymotion {0} is failed", dailymotionContentInfo.cid));
                            }
                        }
                    }
                }
                catch (Exception e)
                {
                    log.logging(e.ToString());
                }
                ds.Clear();
                Thread.Sleep(1000);
            }
        }

        void Run()
        {
            DataSet ds = new DataSet();
            //Waiting for make winform
            Thread.Sleep(5000);            
            log.logging("Service Start...");

            while (!_shouldStop)
            {
                //String cid;
                DMInfo dmInfo = new DMInfo();

                try
                {
                    mapper.GetDMReady(ds);
                    foreach (DataRow r in ds.Tables[0].Rows)
                    {
                        //cid = r["cid"].ToString();
                        dmInfo.cid = r["cid"].ToString();
                        dmInfo.videoid = r["videoid"].ToString();
                        dmInfo.playlistid = r["playlistid"].ToString();
                        dmInfo.old_playlistid = r["old_playlistid"].ToString();
                        dmInfo.title = r["title"].ToString();
                        dmInfo.description = r["description"].ToString();
                        dmInfo.category = r["category"].ToString();
                        dmInfo.tags = r["tag"].ToString();
                        dmInfo.isuse = r["isuse"].ToString();
                        dmInfo.publish_date = r["publish_date"].ToString();
                        dmInfo.expiry_date = r["expiry_date"].ToString();
                        dmInfo.policy_YN = r["policy_YN"].ToString();
                        dmInfo.geoblock_code = r["geoblock_code"].ToString();
                        dmInfo.geoblock_value = r["geoblock_value"].ToString();
                        dmInfo.explicit_YN = r["explicit_YN"].ToString();
                        dmInfo.thumbnail_url = r["thumbnail_url"].ToString();
                        dmInfo.yt_status = r["yt_status"].ToString();

                        // Ready 일 경우
                        // videoid 가 있으면 업데이트
                        if (!String.IsNullOrEmpty(dmInfo.videoid))
                        {
                            if (MetaUpdate(dmInfo))
                            {
                                mapper.UpdateDailyMotionStatus(dmInfo.cid, "Completed");                                
                                mapper.UpdateClipStatus(dmInfo.cid, "Completed");                                
                            }
                            else
                            {
                                if ( String.IsNullOrEmpty(Singleton.getInstance().dm_accesstoken) )
                                {
                                    mapper.UpdateDailyMotionStatus(dmInfo.cid, "Failed");
                                }
                                // accesstoken 을 획득한 경우 실패처리 하지 않고 한번 더 수행
                            }
                        }
                    }
                }
                catch (Exception e)
                {
                    log.logging(e.ToString());
                }
                ds.Clear();
                Thread.Sleep(1000);
            }
            log.logging("Thread Terminate");
        }

        private Boolean MetaUpdate(DMInfo dmInfo)
        {
            //POST /video/{id}
            String url = String.Format("https://api.dailymotion.com/video/{0}", dmInfo.videoid);
            String url_refreshtoken = "https://api.dailymotion.com/oauth/token";
            String response = null;
            String accessToken = Singleton.getInstance().dm_accesstoken;
            String refreshtoken = Singleton.getInstance().dm_refreshtoken;
            String client_id = Singleton.getInstance().dm_client_id;
            String client_secret = Singleton.getInstance().dm_client_secret;

            if (String.IsNullOrEmpty(accessToken))
            {
                NameValueCollection paris = new NameValueCollection();
                paris.Add("grant_type", "refresh_token");
                paris.Add("client_id", client_id);
                paris.Add("client_secret", client_secret);
                paris.Add("refresh_token", refreshtoken);
                accessToken = dmInfo.GetDmRefreshToken(url_refreshtoken, paris);
                Singleton.getInstance().dm_accesstoken = accessToken;
            }

            try
            {
                response = dmInfo.DmEditVideo(url, accessToken);
                log.logging(response);
                return true;
            }
            catch (WebException e)
            {
                String responseText = null;
                var responseStream = e.Response?.GetResponseStream();
                if (responseStream != null)
                {
                    using (var reader = new StreamReader(responseStream))
                    {
                        responseText = reader.ReadToEnd();
                    }
                }
                log.logging(e.ToString());
                log.logging(responseText);

                JObject obj;
                obj = JObject.Parse(responseText);
                log.logging(obj.ToString());
                if ((String)obj["error"]["code"] == "400" || (String)obj["error"]["code"] == "401")
                {
                    // error 면 return false 아니면 refreshtoken 시도
                    NameValueCollection paris = new NameValueCollection();
                    paris.Add("grant_type", "refresh_token");
                    paris.Add("client_id", client_id);
                    paris.Add("client_secret", client_secret);
                    paris.Add("refresh_token", refreshtoken);
                    accessToken = dmInfo.GetDmRefreshToken(url_refreshtoken, paris);
                    Singleton.getInstance().dm_accesstoken = accessToken;
                }
            }            
            return false;
        }
        
    }
}
