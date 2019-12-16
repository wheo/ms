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

namespace MBCPLUS_DAEMON
{
    class YoutubeService
    {        
        private Boolean _shouldStop = false;                        
        private Log log;
        SqlMapper mapper;

        public YoutubeService()
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
                String cid;
                String parseXMLFile = null;
                String transCSVFile = null;
                String csv_destPath = null;
                YoutubeID ytID = new YoutubeID();
                YTtransInfo ytTransInfo = new YTtransInfo();
                try
                {
                    mapper.WaitYoutubeResponse(ds);
                    foreach (DataRow r in ds.Tables[0].Rows)
                    {
                        cid = r["cid"].ToString();
                        ytTransInfo.spoken_language = r["spoken_language"].ToString();
                        ytTransInfo.target_language = r["target_language"].ToString();
                        ytTransInfo.org_lang_title = r["org_lang_title"].ToString();
                        ytTransInfo.org_lang_desc = r["org_lang_desc"].ToString();
                        ytTransInfo.trans_lang_title = r["trans_lang_title"].ToString();
                        ytTransInfo.trans_lang_desc = r["trans_lang_desc"].ToString();
                        ytTransInfo.session_id = r["session_id"].ToString();
                        // Ready 일 경우
                        // 응답 xml 기다림
                        // File Exist Check

                        if ( yt_DownloadResponseXML(cid, ytTransInfo.session_id,  out parseXMLFile) )
                        {
                            if ( yt_GetVideoInfo(parseXMLFile, ytID))
                            {
                                // clip cid status Completed
                                mapper.UpdateClipStatus(cid, "Completed");
                                // 원문언어와 번역언어가 같지 않을 때 번역 추가
                                if ( !String.Equals(ytTransInfo.spoken_language, ytTransInfo.target_language) )
                                {
                                    transCSVFile = yt_MakeTransFerCSVFile(cid, ytID, ytTransInfo);
                                    csv_destPath = String.Format("/{0}_{1}/{2}", cid, ytTransInfo.session_id, Path.GetFileName(transCSVFile));
                                    frmMain.WriteLogThread(String.Format("{0} is created", transCSVFile));
                                    yt_csvSendFile(transCSVFile, csv_destPath);
                                    yt_deliveryCompleteSendFile(cid, ytTransInfo.session_id);
                                }
                                mapper.UpdateYoutubeStatus(cid, "Completed", ytID);
                            }
                            else
                            {
                                // clip cid status Completed
                                mapper.UpdateClipStatus(cid, "Failed");
                                //Parse 후 youtube status update
                                mapper.UpdateYoutubeStatus(cid, "Failed", ytID);
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

        public Boolean yt_deliveryCompleteSendFile(String cid, String session_id)
        {
            yt_SendFile("delivery.complete", String.Format("/{0}_{1}/delivery.complete", cid, session_id));
            return true;
        }

        public Boolean yt_csvSendFile(String csvFileName, String destPath)
        {
            yt_SendFile(csvFileName, destPath);
            return true;
        }

        private Boolean yt_SendFile(String srcPath, String dstPath)
        {
            try
            {
                log.logging(srcPath);
                log.logging(dstPath);
                SessionOptions sessionOptions = new SessionOptions
                {
                    Protocol = Protocol.Sftp,
                    HostName = "partnerupload.google.com",
                    UserName = "yt-mbc-plus-media",
                    PortNumber = 19321,
                    SshPrivateKeyPath = "id-rsa.ppk",
                    GiveUpSecurityAndAcceptAnySshHostKey = true
                };

                using (Session session = new Session())
                {                    
                    // Connect
                    session.Open(sessionOptions);

                    //Upload files
                    TransferOptions transferOptions = new TransferOptions();
                    transferOptions.TransferMode = TransferMode.Binary;
                    transferOptions.OverwriteMode = OverwriteMode.Overwrite;

                    if (!session.FileExists(String.Format("/{0}", Path.GetDirectoryName(dstPath).Replace(@"\", ""))) )
                    {
                        session.CreateDirectory(String.Format("/{0}", Path.GetDirectoryName(dstPath).Replace(@"\", "")));
                    }

                    if (session.FileExists(dstPath))
                    {
                        log.logging(String.Format("{0} File OverWrite", srcPath));
                    }
                    else
                    {
                        log.logging(String.Format("{0} New File", srcPath));
                        // overwrite
                    }

                    TransferOperationResult transferResult;
                    transferResult = session.PutFiles(srcPath, dstPath, false, transferOptions);

                    //Throw on any error
                    transferResult.Check();

                    //Print Result                    
                    foreach (TransferEventArgs e in transferResult.Transfers)
                    {
                        log.logging(String.Format("Upload of {0} is succeeded", e.FileName));
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

        private String yt_MakeTransFerCSVFile(String cid, YoutubeID ytID, YTtransInfo ytTrans)
        {
            //String csvTitle = "video_id,is_primary_language,language,title,description";
            string DirPath = @"csv";

            DirectoryInfo di = new DirectoryInfo(DirPath);

            if (di.Exists != true) Directory.CreateDirectory(DirPath);
            String csvFileName = String.Format(@"{0}\trans-{1}.csv", DirPath, cid);

            Dictionary<String, String> ytMeta = new Dictionary<string, string>();
            ytMeta.Add("video_id", String.Format("\"{0}\"",ytID.VideoID));
            ytMeta.Add("is_primary_language", String.Format("\"{0}\"", "no"));
            ytMeta.Add("language", String.Format("\"{0}\"", ytTrans.target_language));
            ytMeta.Add("title", String.Format("\"{0}\"", ytTrans.trans_lang_title));
            ytMeta.Add("description", String.Format("\"{0}\"",ytTrans.trans_lang_desc));
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

        private Boolean yt_GetVideoInfo(String responseXML, YoutubeID ytID)
        {
            XmlDocument xml = new XmlDocument();
            xml.Load(responseXML);
            XmlNodeList xnList = xml.GetElementsByTagName("action");
            String AssetID = null;
            String videoID = null;
            String status = null;
            foreach (XmlNode xn in xnList)
            {
                if (String.Equals(xn.Attributes["name"].Value, "Parse"))
                {
                    status = xn.InnerText;
                }
                if (String.Equals(xn.Attributes["name"].Value, "Process asset"))
                {
                    AssetID = xn.SelectSingleNode("id").InnerText;
                }
                if (String.Equals(xn.Attributes["name"].Value, "Submit video"))
                {
                    videoID = xn.SelectSingleNode("id").InnerText;
                }
            }
            if ( string.Equals(status, "Success"))
            {
                ytID.status = status;
                ytID.AssetID = AssetID;
                ytID.VideoID = videoID;                
            }
            else
            {
                ytID.status = status;
                ytID.AssetID = "";
                ytID.VideoID = "";                
                return false;
            }
            return true;
        }

        private Boolean yt_DownloadResponseXML(String cid, String session_id, out String parseXML)
        {
            parseXML = null;
            {
                try
                {
                    SessionOptions sessionOptions = new SessionOptions
                    {
                        Protocol = Protocol.Sftp,
                        HostName = "partnerupload.google.com",
                        UserName = "yt-mbc-plus-media",
                        PortNumber = 19321,
                        SshPrivateKeyPath = "id-rsa.ppk",
                        GiveUpSecurityAndAcceptAnySshHostKey = true
                    };
                    Thread.Sleep(5000);
                    using (Session session = new Session())
                    {
                        //session.FileTransferProgress += SessionFileTransferProgress;
                        // Connect
                        session.Open(sessionOptions);

                        //Upload files
                        TransferOptions transferOptions = new TransferOptions();
                        transferOptions.TransferMode = TransferMode.Binary;
                        transferOptions.OverwriteMode = OverwriteMode.Overwrite;
                        String targetFileName = String.Format("/{0}_{1}/status-{0}.csv.xml", cid, session_id);
                        String saveFileName = String.Format(@"yt_status\status-{0}.xml", cid);

                        string DirPath = @"yt_status";

                        DirectoryInfo di = new DirectoryInfo(DirPath);

                        if (di.Exists != true) Directory.CreateDirectory(DirPath);

                        if (session.FileExists(targetFileName))
                        {
                            TransferOperationResult transferResult;
                            transferResult = session.GetFiles(targetFileName, saveFileName, false, transferOptions);
                            //Throw on any error
                            transferResult.Check();
                            //Print Result                            
                            foreach (TransferEventArgs e in transferResult.Transfers)
                            {
                                log.logging(String.Format("Download of {0} is succeeded", e.FileName));
                            }
                            parseXML = saveFileName;
                            return true;
                        }
                        else
                        {
                            log.logging(String.Format("{0} file is not created", targetFileName));
                            return false;
                        }
                    }                 
                }
                catch (Exception e)
                {
                    log.logging(e.ToString());
                    log.logging(e.StackTrace.ToString());
                    return false;
                }
            }            
        }
    }
}
