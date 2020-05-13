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
using System.Globalization;

namespace MBCPLUS_DAEMON
{
    class YoutubeService
    {        
        private Boolean _shouldStop = false;                        
        private Log log;

        void ChannelNPlaylistUpdate()
        {
            //Thread.Sleep(5000);
            SqlMapper mapper = new SqlMapper();
            //기존 채널 삭제해야 함

            DataSet ds = new DataSet();

            mapper.DeleteYTChannelList();
            mapper.GetYTAccountList(ds);
            YTInfo ytInfo = Singleton.getInstance().Get_YTInstance();

            foreach (DataRow r in ds.Tables[0].Rows)
            {
                List<Dictionary<String, String>> list = new List<Dictionary<String, String>>();
                Dictionary<String, Object> map = new Dictionary<String, Object>();

                map = r.Table.Columns
                    .Cast<DataColumn>()
                    .ToDictionary(col => col.ColumnName, col => r.Field<Object>(col.ColumnName));
                ytInfo.authentication(map["keyfile"].ToString(), map["name"].ToString());
                
                try
                {
                    list = ytInfo.GetChannelList();
                } catch(Exception e)
                {
                    log.logging("auth name : " + map["name"].ToString());
                    log.logging(e.ToString());
                }
                mapper.SETYTChannelList(list);
            }

            ds.Clear();
            ds.Dispose();

            ds = new DataSet();
            
            mapper.GetYTChannelList(ds);
            mapper.DeleteYTPlayList();

            foreach (DataRow r in ds.Tables[0].Rows)
            {
                List<Dictionary<String, String>> list = new List<Dictionary<String, String>>();
                list = ytInfo.GetPlayList(r["id"].ToString());                
                for (int i = 0; i < list.Count; i++)
                {
                    mapper.SETYTPlayList(list[i]["id"], list[i]["name"], r["id"].ToString(), i);
                }
            }
            ds.Clear();
        }

        public YoutubeService()
        {   
            log = new Log(this.GetType().Name);
            DoWork();
        }

        void DoWork()
        {
            Thread t1 = new Thread(new ThreadStart(Run));
            t1.Start();
            Thread t2 = new Thread(new ThreadStart(Request));
            t2.Start();
            Thread t3 = new Thread(new ThreadStart(Lists));
            t3.Start();
            Thread t4 = new Thread(new ThreadStart(SendToFTP));
            t4.Start();

            Thread.Sleep(5000);
            try
            {
                ChannelNPlaylistUpdate();
            }
            catch (Exception e)
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
            vo.YoutubeContentInfo youtubeContentInfo = new vo.YoutubeContentInfo();
            
            while(!_shouldStop)
            {
                DataSet ds = new DataSet();
                try
                {
                    if (mapper.YoutubePendingCheck(ds))
                    {
                        foreach (DataRow r in ds.Tables[0].Rows)
                        {
                            youtubeContentInfo.videoid = r["videoid"].ToString();
                            youtubeContentInfo.clip_pk = r["clip_pk"].ToString();
                            youtubeContentInfo.gid = r["gid"].ToString();
                            youtubeContentInfo.cid = r["cid"].ToString();
                            youtubeContentInfo.srcImg = r["srcimg"].ToString();
                            youtubeContentInfo.srcSubtitle = r["srcsubtitle"].ToString();
                            youtubeContentInfo.srcMovie = r["srcmov"].ToString();
                            //FTP 등록
                            if ( !mapper.PutArchiveToFtp(youtubeContentInfo) )
                            {
                                //실패
                                log.logging(String.Format("youtube {0} is failed", youtubeContentInfo.cid));
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

        void Lists()
        {
            SqlMapper mapper;
            mapper = new SqlMapper();
            Thread.Sleep(5000);

            while (!_shouldStop)
            {
                DataSet ds = new DataSet();
                try
                {
                    if (mapper.YoutubeCheckInterFace(ds))
                    {
                        ChannelNPlaylistUpdate();
                        log.logging("Youtube Channel & PlaylistUpdate");
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

        void Request()
        {
            SqlMapper mapper;
            mapper = new SqlMapper();
            Thread.Sleep(5000);

            while (!_shouldStop)
            {
                DataSet ds = new DataSet();
                String cid = null;
                YTMetaInfo ytMetaiInfo = new YTMetaInfo();
                try
                {
                    mapper.YoutubeRequest(ds);
                }
                catch (Exception e)
                {
                    log.logging(e.ToString());
                }

                foreach (DataRow r in ds.Tables[0].Rows)
                {
                    try
                    {
                        cid = r["cid"].ToString();
                        ytMetaiInfo.videoid = r["videoid"].ToString();
                        ytMetaiInfo.channel_id = r["channel_id"].ToString();
                        ytMetaiInfo.title = r["title"].ToString();
                        ytMetaiInfo.description = r["description"].ToString();
                        ytMetaiInfo.category = r["category"].ToString();
                        ytMetaiInfo.tag = r["tag"].ToString();
                        ytMetaiInfo.spoken_language = r["spoken_language"].ToString();
                        ytMetaiInfo.target_language = r["target_language"].ToString();
                        ytMetaiInfo.org_lang_title = r["org_lang_title"].ToString();
                        ytMetaiInfo.org_lang_desc = r["org_lang_desc"].ToString();
                        ytMetaiInfo.trans_lang_title = r["trans_lang_title"].ToString();
                        ytMetaiInfo.trans_lang_desc = r["trans_lang_desc"].ToString();
                        ytMetaiInfo.session_id = r["session_id"].ToString();
                        ytMetaiInfo.thumbnailPath = r["thumbnail"].ToString();
                        ytMetaiInfo.captionPath = r["caption"].ToString();
                        ytMetaiInfo.privacy = r["privacy"].ToString();
                        ytMetaiInfo.start_time = r["start_time"].ToString();

                        if (ytMetaiInfo.start_time.IndexOf("0000-00-00", 0) > 1)
                        {
                            ytMetaiInfo.start_time = null;
                        }
                        YTInfo ytinfo = Singleton.getInstance().Get_YTInstance();
                        // videoid 가 있을 때 수정 Process
                        DataSet ds_account = new DataSet();
                        mapper.GetYTAccountList(ds_account, ytMetaiInfo.channel_id);
                        ytinfo.authentication(ds_account.Tables[0].Rows[0]["keyfile"].ToString(), ds_account.Tables[0].Rows[0]["name"].ToString());

                        //String playlist_id = ytinfo.GetPlaylistFromVideoid(ytMetaiInfo.videoid);
                        if (!ytinfo.Sync_WMS(ytMetaiInfo) )
                        {
                            mapper.UpdateYoutubeVideoIDToNULL(ytMetaiInfo.videoid);
                        }
                        
                        mapper.UpdateFromYtMeta(ytMetaiInfo);
                        mapper.UpdateYoutubeStatus(cid, "Completed");
                        ds_account.Clear();
                    }
                    catch (Exception e)
                    {
                        log.logging(e.ToString());
                    }
                }
                ds.Clear();
                Thread.Sleep(1000);
            }
        }

        void Run()
        {
            SqlMapper mapper;
            mapper = new SqlMapper();
            //Waiting for make winform
            Thread.Sleep(5000);
            log.logging("Service Start...");

            while (!_shouldStop)
            {
                DataSet ds = new DataSet();
                String cid = null;
                String ytReportFile = null;                
                YoutubeID ytID = new YoutubeID();
                YTMetaInfo ytMetaInfo = new YTMetaInfo();
                try
                {
                    mapper.WaitYoutubeReady(ds);
                }
                catch(Exception e)
                {
                    log.logging(e.ToString());
                }
                foreach (DataRow r in ds.Tables[0].Rows)
                {
                    try
                    {
                        cid = r["cid"].ToString();
                        ytMetaInfo.videoid = r["videoid"].ToString();
                        ytMetaInfo.channel_id = r["channel_id"].ToString();
                        ytMetaInfo.title = r["title"].ToString();
                        ytMetaInfo.description = r["description"].ToString();
                        ytMetaInfo.category = r["category"].ToString();
                        ytMetaInfo.tag = r["tag"].ToString();
                        ytMetaInfo.spoken_language = r["spoken_language"].ToString();
                        ytMetaInfo.target_language = r["target_language"].ToString();
                        ytMetaInfo.org_lang_title = r["org_lang_title"].ToString();
                        ytMetaInfo.org_lang_desc = r["org_lang_desc"].ToString();
                        ytMetaInfo.trans_lang_title = r["trans_lang_title"].ToString();
                        ytMetaInfo.trans_lang_desc = r["trans_lang_desc"].ToString();
                        ytMetaInfo.session_id = r["session_id"].ToString();
                        ytMetaInfo.thumbnailPath = r["thumbnail"].ToString();
                        ytMetaInfo.captionPath = r["caption"].ToString();
                        ytMetaInfo.privacy = r["privacy"].ToString();
                        ytMetaInfo.start_time = r["start_time"].ToString();
                        ytMetaInfo.playlist_id = r["playlist_id"].ToString();
                        ytMetaInfo.old_playlist_id = r["old_playlist_id"].ToString();
                        ytMetaInfo.infomation = r["information"].ToString();

                        if ( ytMetaInfo.start_time.IndexOf("0000-00-00",0) > 1)
                        {
                            ytMetaInfo.start_time = null;
                        }

                        // Ready 일 경우
                        // 응답 xml 기다림
                        // File Exist Check

                        YTInfo ytinfo = Singleton.getInstance().Get_YTInstance();

                        if (String.IsNullOrEmpty(ytMetaInfo.videoid))
                        {
                            if (yt_DownloadResponseXML(cid, ytMetaInfo.session_id, out ytReportFile))
                            {
                                ytID.cid = cid;
                                if (yt_GetVideoInfo(ytReportFile, ytID))
                                {
                                    // video_id, asset_id 획득 성공                                    
                                    // 원문언어와 번역언어가 같지 않을 때 번역 추가
                                    if (!String.Equals(ytMetaInfo.spoken_language, ytMetaInfo.target_language))
                                    {
                                        // status 파일에서 추출한 video 를 ytMetaInfo 객체에 반영
                                        ytMetaInfo.videoid = ytID.VideoID;

                                        DataSet ds_account = new DataSet();
                                        mapper.GetYTAccountList(ds_account, ytMetaInfo.channel_id);
                                        ytinfo.authentication(ds_account.Tables[0].Rows[0]["keyfile"].ToString(), ds_account.Tables[0].Rows[0]["name"].ToString());
                                        //ytinfo.Sync_WMS(ytMetaiInfo);
                                        //mapper.UpdateFromYtMeta(ytMetaiInfo);
                                        ytinfo.UpdateVideoInfo(ytMetaInfo);
                                        log.logging(String.Format("UpdateVideoInfo({0}) is completed", ytMetaInfo.videoid));
                                        ds_account.Clear();
                                        /*
                                        transCSVFile = yt_MakeTransFerCSVFile(cid, ytID, ytTransInfo);
                                        csv_destPath = String.Format("/{0}_{1}/{2}", cid, ytTransInfo.session_id, Path.GetFileName(transCSVFile));
                                        frmMain.WriteLogThread(String.Format("{0} is created", transCSVFile));
                                        yt_csvSendFile(transCSVFile, csv_destPath);
                                        yt_deliveryCompleteSendFile(cid, ytTransInfo.session_id);
                                        */
                                    }
                                    mapper.UpdateYoutubeStatus(cid, "Completed", ytID);
                                    // clip cid status Completed
                                    mapper.UpdateClipStatus(cid, "Completed");
                                }
                                else
                                {
                                    // clip cid status Completed
                                    /* Youtube 실패하더라도 영향을 주면 안됨 (중요) 2019-03-14
                                    mapper.UpdateClipStatus(cid, "Failed");
                                    */
                                    //Parse 후 youtube status update
                                    mapper.UpdateYoutubeStatus(cid, "Failed", ytID);
                                }
                            }
                        }
                        else
                        {
                            // videoid 가 있을 때 수정 Process
                            DataSet ds_account = new DataSet();
                            mapper.GetYTAccountList(ds_account, ytMetaInfo.channel_id);
                            ytinfo.authentication(ds_account.Tables[0].Rows[0]["keyfile"].ToString(), ds_account.Tables[0].Rows[0]["name"].ToString());                            
                            ytinfo.UpdateVideoInfo(ytMetaInfo);
                            ytinfo.UpdatePlaylistitem(ytMetaInfo);
                            ytinfo.SetThumbNail(ytMetaInfo);
                            mapper.UpdateYoutubeStatus(cid, "Completed");
                            mapper.UpdateClipStatus(cid, "Completed");
                            log.logging(String.Format("UpdateVideoInfo({0}) is completed", ytMetaInfo.videoid));
                        }
                    }

                    catch (Exception e)
                    {
                        if (cid != null)
                        {
                            mapper.UpdateYoutubeStatus(cid, "Failed");
                        }
                        log.logging(e.ToString());
                    }
                }                               
                ds.Clear();
                Thread.Sleep(30000);
            }            
            log.logging("Thread Terminate");
        }

        /*
        public Boolean yt_deliveryCompleteSendFile(String cid, String session_id)
        {
            yt_SendFile("delivery.complete", String.Format("/{0}_{1}/delivery.complete", cid, session_id));
            return true;
        }
        */

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

        /*
        private String yt_MakeTransFerCSVFile(String cid, YoutubeID ytID, YTMetaInfo ytTrans)
        {
            //String csvTitle = "video_id,is_primary_language,language,title,description";
            string DirPath = @"csv";

            DirectoryInfo di = new DirectoryInfo(DirPath);

            if (di.Exists != true) Directory.CreateDirectory(DirPath);
            String csvFileName = String.Format(@"{0}\trans-{1}.csv", DirPath, cid);

            //특수문자 추가 패턴 2018-07-25 " 를 ""로
            ytTrans.trans_lang_title = ytTrans.trans_lang_title.Replace("\"", "\"\"");
            ytTrans.trans_lang_desc = ytTrans.trans_lang_desc.Replace("\"", "\"\"");

            log.logging(ytTrans.trans_lang_title);
            log.logging(ytTrans.trans_lang_desc);            

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
        */

        private Boolean yt_GetVideoInfo(String reportFile, YoutubeID ytID)
        {
            //Row number, Status, Video ID,Custom ID, Video file,Asset ID
            using (var stream = File.OpenRead(reportFile))
            {
                using (var reader = new StreamReader(stream))
                {
                    var data = CsvParser.ParseHeadAndTail(reader, ',', '"');

                    var header = data.Item1;
                    var lines = data.Item2;

                    foreach (var line in lines)
                    {
                        for( var i = 0; i < header.Count; i++)
                        {
                            if (!string.IsNullOrEmpty(line[i]))
                            {
                                log.logging(String.Format("{0}:{1}", header[i], line[i]));
                                if (string.Equals(header[i], "Status"))
                                {
                                    if (string.Equals(line[i], "Successful"))
                                    {
                                        ytID.status = line[i];
                                    }
                                    else
                                    {
                                        log.logging(String.Format("{0} youtube status : {1}", ytID.cid, line[i]));
                                        return false;
                                    }
                                }                                
                                if (string.Equals(header[i], "Video ID") || string.Equals(header[i], "Reference ID"))
                                {
                                    ytID.VideoID = line[i];
                                }
                                if (String.Equals(header[i], "Asset ID"))
                                {
                                    ytID.AssetID = line[i];
                                }
                            }
                            else if ( String.IsNullOrEmpty(header[i]))
                            {
                                // header[i]가 공백일 경우
                                return false;
                            }
                        }
                    }
                }
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
                        String targetFileName = String.Format("/{0}_{1}/report-{0}.csv", cid, session_id);
                        String saveFileName = String.Format(@"yt_report\report-{0}.csv", cid);

                        string DirPath = @"yt_report";

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
