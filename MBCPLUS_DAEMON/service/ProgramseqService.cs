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
    class ArchiveProgramService
    {
        private Boolean _shouldStop = false;        
        //private String m_imgsrcpath;
        //private String m_clipsrcpath;
        //private String m_dstpath;        
        private String m_pk;        
        private String m_sql = "";
        private ConnectionPool connPool;
        private SqlMapper mapper;

        private Log log;

        public ArchiveProgramService()
        {
            // put this className
            mapper = new SqlMapper();
            log = new Log(this.GetType().Name);
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
            String status = null;
            MySqlCommand cmd;

            connPool = new ConnectionPool();
            connPool.SetConnection(new MySqlConnection(Singleton.getInstance().GetStrConn()));

            String strBaseUri = "http://metaapi.mbcmedia.net:5000/SMRMetaCollect.svc/";
            
            //Waiting for make winform
            Thread.Sleep(5000);
            //frmMain.WriteLogThread("Program Service Start...");
            log.logging("Service Start...");

            while (!_shouldStop)
            {
                try
                {
                    mapper.GetProgramSeqService(ds);

                    foreach (DataRow r in ds.Tables[0].Rows)
                    {
                        String mediadomain = "http://Img.mbcmpp.co.kr"; // 2017-11-22 적용
                        //String mediadomain = "http://mbcplus-dn.dl.cdn.cloudn.co.kr";
                        m_pk = r["program_seq_pk"].ToString();
                        String contentid = r["contentid"].ToString();
                        String onair_date = r["onair_date"].ToString();
                        String userid = r["userid"].ToString();
                        String originid = r["originid"].ToString();
                        String phun_onair_ymd = r["phun_onair_ymd"].ToString();
                        String cornerid = r["cornerid"].ToString();
                        String smr_pid = r["smr_pid"].ToString();
                        String programid = r["programid"].ToString();
                        String contentnumber = r["contentnumber"].ToString();
                        String cornernumber = r["cornernumber"].ToString();
                        String preview = r["preview"].ToString();
                        String broaddate = r["broaddate"].ToString();
                        String title = r["title"].ToString();                        
                        String searchkeyword = r["searchkeyword"].ToString();
                        String actor = r["actor"].ToString();
                        String targetage = r["targetage"].ToString();
                        String targetnation = r["targetnation"].ToString();
                        String targetplatform = r["targetplatform"].ToString();
                        String limitnation  = r["limitnation"].ToString();
                        String platformisuse = r["platformisuse"].ToString();
                        String genre = r["genre"].ToString();
                        String isuse = r["isuse"].ToString();
                        String tempyn = r["tempyn"].ToString();
                        String phun_ch = r["phun_ch"].ToString();
                        String phun_ps = r["phun_ps"].ToString();
                        String phun_case = r["phun_case"].ToString();
                        String phun_pgm_seq = r["phun_pgm_seq"].ToString();
                        String cdnurl_img = r["cdnurl_img"].ToString();
                        String contentimg = cdnurl_img.Replace(mediadomain, "");

                        // ADD log
                        String imgsrcpath = r["imgsrcpath"].ToString();
                        String orgimgname = r["orgimgname"].ToString();

                        log.logging("[ProgramseqService] imgsrcpath : " + imgsrcpath);
                        log.logging("[ProgramseqService] orgimgname : " + orgimgname);

                        status = r["status"].ToString();
                        if (status.Equals("Ready"))
                        {
                            connPool.ConnectionOpen();
                            m_sql = String.Format("UPDATE TB_PROGRAM_SEQ SET starttime = CURRENT_TIMESTAMP(), status = 'Running' WHERE program_seq_pk = '{0}'", m_pk);
                            //Running 으로 변경
                            cmd = new MySqlCommand(m_sql, connPool.getConnection());
                            cmd.ExecuteNonQuery();
                            connPool.ConnectionClose();

                            frmMain.WriteLogThread(String.Format(@"program_seq_pk({0}) is Running", m_pk));

                            JObject metaJson = new JObject(
                                new JProperty("contentid", contentid),
                                new JProperty("onair_date", onair_date),                                
                                new JProperty("userid", userid),
                                new JProperty("originid", originid),
                                new JProperty("phun_onair_ymd", phun_onair_ymd),
                                new JProperty("cornerid", cornerid),
                                new JProperty("contentimg", contentimg),
                                new JProperty("programid", smr_pid), //smr_pid로 변경
                                new JProperty("contentnumber", contentnumber),
                                new JProperty("cornernumber", cornernumber),
                                new JProperty("preview", preview),
                                new JProperty("broaddate", broaddate),
                                new JProperty("title", title),
                                new JProperty("searchkeyword", searchkeyword),
                                new JProperty("actor", actor),
                                new JProperty("targetage", targetage),
                                new JProperty("targetnation", targetnation),
                                new JProperty("targetplatform", targetplatform),
                                new JProperty("limitnation", limitnation),
                                new JProperty("platformisuse", platformisuse),
                                new JProperty("genre", genre),
                                new JProperty("isuse", isuse),
                                new JProperty("tempyn", tempyn),
                                new JProperty("phun_ch", phun_ch),
                                new JProperty("phun_ps", phun_ps),
                                new JProperty("phun_case", phun_case),
                                new JProperty("phun_pgm_seq", phun_pgm_seq)
                                );

                            String strJson = "";
                            String uri = "";
                            if (!String.IsNullOrEmpty(contentid))
                            {
                                // contentid 가 있으면 Update
                                uri = strBaseUri + "UpdateContentMeta";
                                strJson = metaJson.ToString();
                            }
                            else
                            {
                                //contentid 가 없으면 신규
                                uri = strBaseUri + "CreateContentMeta";
                                strJson = metaJson.ToString();                                
                            }
                            log.logging(uri);
                            log.logging(strJson);
                            var response = Http.Post(uri, strJson);
                            string responseString = Encoding.UTF8.GetString(response);
                            log.logging(responseString);

                            // 성공시 Completed
                            //JObject obj = JObject.Parse(responseString);
                            JArray arr = JArray.Parse(responseString);                            
                            String primarykey = "";
                            String successed = "";
                            Boolean metaSuccess = true;

                            foreach (JObject o in arr.Children<JObject>())
                            {
                                foreach (JProperty p in o.Properties())
                                {
                                    log.logging(p.Name + " | " + (String)p.Value);
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
                            log.logging("primarykey is " + primarykey);
                            if (metaSuccess)
                            {
                                connPool.ConnectionOpen();
                                if (primarykey == "1")
                                {
                                    m_sql = String.Format("UPDATE TB_PROGRAM_SEQ SET endtime = CURRENT_TIMESTAMP(), status = 'Completed' WHERE program_seq_pk = '{0}'", m_pk);
                                }
                                else if (!String.IsNullOrEmpty(primarykey))
                                {
                                    m_sql = String.Format("UPDATE TB_PROGRAM_SEQ SET endtime = CURRENT_TIMESTAMP(), status = 'Completed', contentid = '{0}' WHERE program_seq_pk = '{1}'", primarykey, m_pk);
                                }
                                
                                //Completed 으로 변경
                                cmd = new MySqlCommand(m_sql, connPool.getConnection());
                                cmd.ExecuteNonQuery();
                                connPool.ConnectionClose();
                                frmMain.WriteLogThread(String.Format(@"program_seq_pk({0}) is Completed", m_pk));
                            }
                            else
                            {
                                connPool.ConnectionOpen();
                                m_sql = String.Format("UPDATE TB_PROGRAM_SEQ SET endtime = CURRENT_TIMESTAMP(), status = 'Failed' WHERE program_seq_pk = '{0}'", m_pk);
                                //Failed로 변경
                                cmd = new MySqlCommand(m_sql, connPool.getConnection());
                                cmd.ExecuteNonQuery();
                                connPool.ConnectionClose();
                                frmMain.WriteLogThread(String.Format(@"program_seq_pk({0}) is Failed", m_pk));
                            }
                        }
                    }
                }
                catch (Exception e)
                {
                    frmMain.WriteLogThread(e.ToString());
                    log.logging(e.ToString());
                }
                Thread.Sleep(1000);
                ds.Clear();
            }
            connPool.ConnectionDisPose();
            log.logging("Thread Terminate");
        }
    }
}
