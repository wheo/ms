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

        void DoWork()
        {
            Thread t1 = new Thread(new ThreadStart(Run));
            t1.Start();
        }

        public void RequestStop()
        {
            _shouldStop = true;
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

                        // Ready 일 경우
                        // videoid 가 있으면 업데이트
                        if (!String.IsNullOrEmpty(dmInfo.videoid))
                        {
                            if (MetaUpdate(dmInfo))
                            {
                                mapper.UpdateDailymotionStatus(dmInfo.cid, "Completed");
                            }
                            else
                            {
                                // accesstoken 이 없는 경우 다음 turn으로 넘김 따로 실패 처리하지 않음
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
                if ((String)obj["error"]["code"] == "400")
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
