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

namespace MBCPLUS_DAEMON
{
    internal class ClipService
    {
        private Boolean _shouldStop = false;

        //private String m_imgsrcpath;
        //private String m_clipsrcpath;
        //private String m_dstpath;
        private String m_pk;

        private SqlMapper mapper;

        private Log log;

        private static Object ClipLock = new Object(); // log lock object

        public ClipService()
        {
            //put this Class Name
            mapper = new SqlMapper();
            log = new Log(this.GetType().Name);
            DoWork();
        }

        private void DoWork()
        {
            Thread t1 = new Thread(new ThreadStart(Run));
            t1.Start();
        }

        public void RequestStop()
        {
            _shouldStop = true;
        }

        private void Run()
        {
            DataSet ds = new DataSet();
            DataSet ds_clip_YN = new DataSet();

            String strBaseUri = "http://metaapi.mbcmedia.net:5000/SMRMetaCollect.svc/";

            //Waiting for make winform
            Thread.Sleep(5000);
            //frmMain.WriteLogThread("Clip Service Start...");
            log.logging("Service Start...");

            while (!_shouldStop)
            {
                try
                {
                    mapper.GetClipService(ds);
                    foreach (DataRow r in ds.Tables[0].Rows)
                    {
                        m_pk = r["clip_pk"].ToString();
                        String cid = r["cid"].ToString(); // cid 때문에 문제 생기면 지우기
                        String edit_date = r["edit_date"].ToString();
                        String userid = r["userid"].ToString();
                        String contentid = r["contentid"].ToString(); ;
                        String clipid = r["clipid"].ToString();
                        String cliporder = r["cliporder"].ToString();
                        String title = r["title"].ToString();
                        String synopsis = r["synopsis"].ToString();
                        String searchkeyword = r["searchkeyword"].ToString();
                        //String mediadomain = "http://mbcplus-dn.dl.cdn.cloudn.co.kr";
                        String mediadomain = "http://mov.mbcmpp.co.kr"; // 2017-11-22 재개
                        String img_mediadomain = "http://Img.mbcmpp.co.kr"; // 2017-11-22 재개
                        String itemtypeid = r["itemtypeid"].ToString();
                        String cliptype = r["cliptype"].ToString();
                        String clipcategory = r["clipcategory"].ToString();
                        String subcategory = r["subcategory"].ToString();
                        String playtime = r["playtime"].ToString();
                        String starttime = r["starttime"].ToString();
                        String endtime = r["endtime"].ToString();
                        String returnlink = r["returnlink"].ToString();
                        String targetage = r["targetage"].ToString();
                        String targetnation = r["targetnation"].ToString();
                        String targetplatform = r["targetplatform"].ToString();
                        String limitnation = r["limitnation"].ToString();
                        String isuse = r["isuse"].ToString();
                        String channelid = r["channelid"].ToString();
                        String hashtag = r["hashtag"].ToString();
                        String filemodifydate = r["filemodifydate"].ToString();
                        String reservedate = r["reservedate"].ToString();
                        String linktitle1 = r["linktitle1"].ToString();
                        String linkurl1 = r["linkurl1"].ToString();
                        String linktitle2 = r["linktitle2"].ToString();
                        String linkurl2 = r["linkurl2"].ToString();
                        String linktitle3 = r["linktitle3"].ToString();
                        String linkurl3 = r["linkurl3"].ToString();
                        String linktitle4 = r["linktitle4"].ToString();
                        String linkurl4 = r["linkurl4"].ToString();
                        String linktitle5 = r["linktitle5"].ToString();
                        String linkurl5 = r["linkurl5"].ToString();
                        String isfullvod = r["isfullvod"].ToString();
                        String targetplatformvalue = r["targetplatformvalue"].ToString();
                        String broaddate = r["broaddate"].ToString();
                        String sportscomment = r["sportscomment"].ToString();
                        String platformisuse = r["platformisuse"].ToString();
                        String masterclipyn = r["masterclipyn"].ToString();
                        String cdnurl_img = r["cdnurl_img"].ToString();
                        String cdnurl_mov = r["cdnurl_mov"].ToString();
                        String filepath = cdnurl_mov.Replace(mediadomain, "");
                        String contentimg = cdnurl_img.Replace(img_mediadomain, "");
                        String actor = r["actor"].ToString();
                        //String contentimg = "/ATTACHMENT/SMR/IMAGE/A000000308/2016/10/17/T9201610170109.jpg";

                        //ADD LOG
                        String orgimgname = r["orgimgname"].ToString();
                        String orgclipname = r["orgclipname"].ToString();
                        String imgsrcpath = r["imgsrcpath"].ToString();
                        String clipsrcpath = r["clipsrcpath"].ToString();

                        log.logging("[CLIPService] orgimgname : " + orgimgname);
                        log.logging("[CLIPService] orgclipname : " + orgclipname);
                        log.logging("[CLIPService] imgsrcpath : " + imgsrcpath);
                        log.logging("[CLIPService] clipsrcpath : " + clipsrcpath);

                        //log.logging(m_sql);
                        //Ready 상태 찾기
                        try
                        {
                            //Ready -> MetaSending 으로 변경
                            string sql = String.Format("UPDATE TB_CLIP SET starttime = CURRENT_TIMESTAMP(), status = 'MetaSending' WHERE clip_pk = '{0}'", m_pk);

                            using (MySqlConnection conn = new MySqlConnection(Singleton.getInstance().GetStrConn()))
                            {
                                conn.Open();
                                MySqlCommand cmd = new MySqlCommand(sql, conn);
                                cmd.ExecuteNonQuery();
                            }

                            frmMain.WriteLogThread(String.Format(@"[ClipService] clip_pk({0}) is Running", m_pk));
                            JObject metaJson = new JObject(
                                new JProperty("contentid", contentid),
                                new JProperty("edit_date", edit_date),
                                new JProperty("userid", userid),
                                new JProperty("clipid", clipid),
                                new JProperty("cliporder", cliporder),
                                new JProperty("title", title),
                                new JProperty("synopsis", synopsis),
                                new JProperty("actor", actor),
                                new JProperty("searchkeyword", searchkeyword),
                                new JProperty("mediadomain", mediadomain),
                                new JProperty("filepath", filepath),
                                new JProperty("itemtypeid", itemtypeid),
                                new JProperty("cliptype", cliptype),
                                new JProperty("clipcategory", clipcategory),
                                new JProperty("subcategory", subcategory),
                                new JProperty("contentimg", contentimg),
                                new JProperty("playtime", playtime),
                                new JProperty("starttime", starttime),
                                new JProperty("endtime", endtime),
                                new JProperty("returnlink", returnlink),
                                new JProperty("targetage", targetage),
                                new JProperty("targetnation", targetnation),
                                new JProperty("targetplatform", targetplatform),
                                new JProperty("limitnation", limitnation),
                                new JProperty("isuse", isuse),
                                new JProperty("channelid", channelid),
                                new JProperty("hashtag", hashtag),
                                new JProperty("filemodifydate", filemodifydate),
                                new JProperty("reservedate", reservedate),
                                new JProperty("linktitle1", linktitle1),
                                new JProperty("linkurl1", linkurl1),
                                new JProperty("linktitle2", linktitle2),
                                new JProperty("linkurl2", linkurl2),
                                new JProperty("linktitle3", linktitle3),
                                new JProperty("linkurl3", linkurl3),
                                new JProperty("linktitle4", linktitle4),
                                new JProperty("linkurl4", linkurl4),
                                new JProperty("linktitle5", linktitle5),
                                new JProperty("linkurl5", linkurl5),
                                new JProperty("isfullvod", isfullvod),
                                new JProperty("targetplatformvalue", targetplatformvalue),
                                new JProperty("broaddate", broaddate),
                                new JProperty("sportscomment", sportscomment),
                                new JProperty("platformisuse", platformisuse),
                                new JProperty("masterclipyn", masterclipyn)
                                );

                            String strJson = "";
                            String uri = "";
                            //frmMain.WriteLogThread("clip id is " + clipid);
                            if (!String.IsNullOrEmpty(clipid))
                            {
                                // clipid 가 있으면 Update
                                uri = strBaseUri + "UpdateClipMediaMeta";
                                strJson = metaJson.ToString();
                            }
                            else
                            {
                                //clipid 가 없으면 신규
                                uri = strBaseUri + "CreateClipMediaMeta";
                                strJson = metaJson.ToString();
                            }

                            log.logging(uri);
                            log.logging(strJson);
                            var response = Http.Post(uri, strJson);
                            string responseString = Encoding.UTF8.GetString(response);
                            log.logging(responseString);

                            //성공시 Completed
                            //JObject obj = JObject.Parse(responseString);
                            JArray arr = JArray.Parse(responseString);
                            //String errcode = "";
                            //String errmsg = "";
                            String primarykey = "";
                            String successed = "";
                            Boolean metaSuccess = true;

                            foreach (JObject o in arr.Children<JObject>())
                            {
                                foreach (JProperty p in o.Properties())
                                {
                                    log.logging(String.Format("[ClipService]({0}) {1}|{2}", cid, p.Name, p.Value));
                                    if (p.Name == "primarykey")
                                    {
                                        primarykey = (String)p.Value;
                                    }
                                    if (p.Name == "successed")
                                    {
                                        successed = (String)p.Value;
                                    }
                                }
                                if (successed == "False")
                                {
                                    metaSuccess = false;
                                }
                            }
                            log.logging(String.Format("[ClipService] ({0}) primarykey is : {1}", cid, primarykey));

                            string _sql = "";

                            if (metaSuccess)
                            {
                                // primarykey 1 이면 update
                                if (primarykey == "1")
                                {
                                    _sql = String.Format("UPDATE TB_CLIP SET endtime = CURRENT_TIMESTAMP(), status = 'Completed' WHERE clip_pk = '{0}'", m_pk);
                                }
                                else if (!String.IsNullOrEmpty(primarykey)) // primarykey 가 있으면 update
                                {
                                    _sql = String.Format("UPDATE TB_CLIP SET endtime = CURRENT_TIMESTAMP(), status = 'Completed', clipid = '{0}' WHERE clip_pk = '{1}'", primarykey, m_pk);
                                }
                                //Completed 으로 변경
                                using (MySqlConnection conn = new MySqlConnection(Singleton.getInstance().GetStrConn()))
                                {
                                    conn.Open();
                                    MySqlCommand cmd = new MySqlCommand(_sql, conn);
                                    cmd.ExecuteNonQuery();
                                }
                                frmMain.WriteLogThread(String.Format(@"[ClipService] clip_pk({0}) is Completed", m_pk));
                                log.logging(String.Format(@"[ClipService] clip_pk({0}) is Completed", m_pk));
                            }
                            else
                            {
                                using (MySqlConnection conn = new MySqlConnection(Singleton.getInstance().GetStrConn()))
                                {
                                    conn.Open();
                                    _sql = String.Format("UPDATE TB_CLIP SET endtime = CURRENT_TIMESTAMP(), status = 'Failed' WHERE clip_pk = '{0}'", m_pk);
                                    //Failed로 변경
                                    MySqlCommand cmd = new MySqlCommand(_sql, conn);
                                    cmd.ExecuteNonQuery();
                                }
                                frmMain.WriteLogThread(String.Format(@"clip_pk({0}) is Failed", m_pk));
                            }
                        }
                        catch (Exception e)
                        {
                            using (MySqlConnection conn = new MySqlConnection(Singleton.getInstance().GetStrConn()))
                            {
                                conn.Open();
                                string sql = String.Format("UPDATE TB_CLIP SET endtime = CURRENT_TIMESTAMP(), status = 'Failed' WHERE clip_pk = '{0}'", m_pk);
                                //Failed로 변경
                                MySqlCommand cmd = new MySqlCommand(sql, conn);
                                cmd.ExecuteNonQuery();
                            }
                            frmMain.WriteLogThread(String.Format(@"[ClipService] clip_pk({0}) is Failed", m_pk));
                            log.logging(String.Format(@"[ClipService] clip_pk({0}) is Failed", m_pk));
                            log.logging("[ClipService] " + e.ToString());
                        }
                    }
                }
                catch (Exception e)
                {
                    //frmMain.WriteLogThread("[ClipService] " + e.ToString());
                    log.logging(e.ToString());
                }
                Thread.Sleep(1000);
                ds.Clear();
            }

            log.logging("Thread Terminate");
        }
    }
}