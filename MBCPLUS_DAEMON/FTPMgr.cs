using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Net;
using System.IO;
using System.Web;
using System.Data;
using System.Xml;
using System.Threading;
using WinSCP;
using MySql.Data;
using MySql.Data.MySqlClient;
using System.Collections.Specialized;
using Newtonsoft.Json.Linq;
using System.Net.Http;

namespace MBCPLUS_DAEMON
{
    internal class FTPMgr
    {
        private String m_strSourcePath;
        private String m_strTargetPath;
        private String m_uploadPath;
        private String m_deletePath;
        private String m_strTargetPathWithoutFileName;
        private String m_ftppath;
        private String m_host;
        private String m_path;
        private String m_id;
        private String m_pw;
        private String m_customer_id;
        private String m_port;
        private String m_alais_YN;
        private String m_gid;
        private String m_cid;
        private String m_pk;
        private Log log;
        private SqlMapper mapper;
        private int retryLimitCount = 5;

        public FTPMgr()
        {
            mapper = new SqlMapper();
            log = new Log(this.GetType().Name);
            m_alais_YN = "N";
        }

        public void SetSourcePath(String srcpath)
        {
            m_strSourcePath = srcpath;
        }

        public void SetAlias(String alias_YN)
        {
            m_alais_YN = alias_YN;
        }

        public void SetTargetPath(String dstpath)
        {
            m_strTargetPath = dstpath;

            if (m_customer_id == "1" || m_customer_id == "2")
            {
                m_strTargetPathWithoutFileName = Path.GetDirectoryName(m_strTargetPath).Replace(@"\", "/");
                m_ftppath = m_strTargetPathWithoutFileName;
                m_uploadPath = String.Format("{0}:{1}/{2}", m_host, m_port, m_strTargetPath);
            }
            else if (m_alais_YN == "N")
            {
                m_path = String.Format(@"{0}/{1}", m_path, m_gid);
                m_uploadPath = String.Format("{0}:{1}/{2}/{3}", m_host, m_port, m_path, Path.GetFileName(m_strTargetPath));
                m_ftppath = m_path;
            }
            else if (m_alais_YN == "Y")
            {
                m_uploadPath = String.Format("{0}:{1}/{2}/{3}", m_host, m_port, m_path, Util.replaceSpaceChar(m_strTargetPath));
                m_ftppath = String.Format(@"{0}/{1}", m_path, Util.replaceSpaceChar(Path.GetDirectoryName(m_strTargetPath).Replace(@"\", "/")));
            }
            frmMain.WriteLogThread("[FTPMgr] m_ftp_path : " + m_ftppath);
            frmMain.WriteLogThread("[FTPMgr] m_uploadpath : " + m_uploadPath);
        }

        public void SetConfidential(String pk, String host, String path, String id, String pw, String port, String customer_id, String gid, String cid)
        {
            m_pk = pk;
            m_host = host;
            m_path = path;
            m_id = id;
            m_pw = pw;
            m_port = port;
            m_customer_id = customer_id;
            m_gid = gid;
            m_cid = cid;
        }

        private void SessionFileTransferProgress(object sender, FileTransferProgressEventArgs e)
        {
            //log.logging(String.Format("FileName : {0} ({1})", e.FileName, e.FileProgress));
            mapper.UpdateSendingProgress(m_pk, (double)e.FileProgress * 100);
        }

        public Boolean SendDailymotion(String type, DMInfo dmInfo)
        {
            try
            {
                Singleton.getInstance().dm_accesstoken = null;
                String accesstoken = Singleton.getInstance().dm_accesstoken;
                //String refreshtoken = "03e8df4a23fcae3fbc2243302f39e415ba324740";
                //String client_id = "31aa0be41e6a19e42204";
                //String client_secret = "9be3d4dab81a28da00afb100fb86d1de85144294";
                String refreshtoken = Singleton.getInstance().dm_refreshtoken;
                String client_id = Singleton.getInstance().dm_client_id;
                String client_secret = Singleton.getInstance().dm_client_secret;

                String get_upload_url = "https://api.dailymotion.com/file/upload";
                String url_refreshtoken = "https://api.dailymotion.com/oauth/token";
                String url_create = "https://api.dailymotion.com/me/videos";
                String url_video = "https://api.dailymotion.com/video/"; //video id required

                if (String.IsNullOrEmpty(accesstoken))
                {
                    NameValueCollection paris = new NameValueCollection();
                    paris.Add("grant_type", "refresh_token");
                    paris.Add("client_id", client_id);
                    paris.Add("client_secret", client_secret);
                    paris.Add("refresh_token", refreshtoken);
                    accesstoken = dmInfo.GetDmRefreshToken(url_refreshtoken, paris);
                    Singleton.getInstance().dm_accesstoken = accesstoken;
                }

                var response = "";
                response = dmInfo.GetDmVideoUrl(get_upload_url, accesstoken);

                log.logging(response);

                JObject obj;
                obj = JObject.Parse(response);
                String upload_url = (String)obj["upload_url"];
                String progress_url = (String)obj["progress_url"];
                response = dmInfo.DmVideoUpload(upload_url, progress_url, m_strSourcePath, m_pk);
                log.logging(response);

                obj = JObject.Parse(response);
                String videourl = (String)obj["url"];

                String videoid = null;
                // create video
                response = dmInfo.DmCreateVideo(url_create, videourl, accesstoken);
                log.logging(response);

                obj = JObject.Parse(response);
                videoid = (String)obj["id"];
                mapper.UpdateDMVideoid(m_cid, videoid);
                String[] playlist_ids = dmInfo.playlistid.Split(',');
                String PlaylistURL = null;
                if (playlist_ids.Length > 0)
                {
                    foreach (String playlist_id in playlist_ids)
                    {
                        if (!String.IsNullOrEmpty(playlist_id))
                        {
                            PlaylistURL = String.Format("https://api.dailymotion.com/playlist/{0}/videos/{1}", playlist_id, videoid);
                            //linking playlist
                            //accesstoken 만료 주의(고용량 전송시 버그 유발될 수 있음)
                            response = dmInfo.SetDMPlaylist(PlaylistURL, accesstoken);
                            log.logging("SetDMPlaylist : " + PlaylistURL);
                        }
                    }
                }
                url_video = String.Format("{0}{1}", url_video, videoid);
                response = dmInfo.DmPublishVideo(url_video, accesstoken);
                log.logging(response);

                String captionUrl = mapper.GetCaption(m_cid);
                if (!String.IsNullOrEmpty(captionUrl))
                {
                    String setCaptionUrl = String.Format(@"https://api.dailymotion.com/video/{0}/subtitles", videoid);
                    NameValueCollection param = new NameValueCollection();
                    param.Add("format", "SRT");
                    param.Add("language", "ko");
                    param.Add("url", captionUrl);
                    log.logging("SetCaption : " + setCaptionUrl);
                    response = dmInfo.SetCaption(setCaptionUrl, param, accesstoken);
                    log.logging("caption response : " + response);
                }
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
                Singleton.getInstance().dm_accesstoken = null;
                return false;
            }
            return true;
        }

        public Boolean SendFile(String yt_flag, String type, YTInfo ytInfo)
        {
            if (String.IsNullOrEmpty(m_id) | String.IsNullOrEmpty(m_pk))
            {
                // ERROR_CODE
                return false;
            }
            //String destPath = String.Format("/{0}_{1}/{2}", Path.GetFileNameWithoutExtension(m_strSourcePath), ytInfo.session_id, Path.GetFileName(m_strSourcePath));
            String destPath = String.Format("/{0}_{1}/{2}", ytInfo.cid, ytInfo.session_id, Path.GetFileName(m_strSourcePath));


            if (yt_SendFile(m_strSourcePath, destPath))
            {
                if (Path.GetExtension(m_strSourcePath).ToLower() == ".mp4")
                {
                    ytInfo.movpath = m_strSourcePath;
                    ytInfo.IsMovCompleted = true;
                }
                else if (Path.GetExtension(m_strSourcePath).ToLower() == ".jpg" || Path.GetExtension(m_strSourcePath).ToLower() == ".png")
                {
                    ytInfo.custom_thumbnail = Path.GetFileName(m_strSourcePath);
                }
                else if (Path.GetExtension(m_strSourcePath).ToLower() == ".srt")
                {
                    ytInfo.caption_file = Path.GetFileName(m_strSourcePath);
                }
                return true;
            }
            else
            {
                return false;
            }
        }

        public Boolean yt_deliveryCompleteSendFile(String destPath)
        {
            yt_SendFile("delivery.complete", String.Format("/{0}/delivery.complete", Path.GetDirectoryName(destPath).Replace(@"\", "")));
            return true;
        }

        public Boolean yt_csvSendFile(String csvFileName, String destPath)
        {
            yt_SendFile(csvFileName, destPath);
            return true;
        }

        private Boolean yt_SendFile(String srcPath, String dstPath)
        {
            int retryCount = 0;
            while (retryLimitCount > retryCount)
            {
                try
                {
                    log.logging(srcPath);
                    log.logging(dstPath);
                    SessionOptions sessionOptions = new SessionOptions
                    {
                        Protocol = Protocol.Sftp,
                        HostName = m_host,
                        UserName = m_id,
                        PortNumber = Convert.ToInt32(m_port),
                        SshPrivateKeyPath = "id-rsa.ppk",
                        GiveUpSecurityAndAcceptAnySshHostKey = true
                    };

                    using (Session session = new Session())
                    {
                        session.FileTransferProgress += SessionFileTransferProgress;
                        // Connect
                        session.Open(sessionOptions);

                        //Upload files
                        TransferOptions transferOptions = new TransferOptions();
                        transferOptions.TransferMode = TransferMode.Binary;
                        transferOptions.OverwriteMode = OverwriteMode.Overwrite;

                        String makeDIR = String.Format("/{0}", Path.GetDirectoryName(dstPath).Replace(@"\", ""));

                        if (!session.FileExists(makeDIR))
                        {
                            session.CreateDirectory(makeDIR);
                        }

                        if (session.FileExists(dstPath))
                        {
                            log.logging(String.Format("{0} File OverWrite", dstPath));
                        }
                        else
                        {
                            log.logging(String.Format("{0} New File", dstPath));
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
                    return true;
                }
                catch (Exception e)
                {
                    log.logging(e.ToString());
                    log.logging(String.Format("Retry Count : {0}", retryCount));
                    //return false;
                    retryCount++;
                }
            }
            return false;
        }

        public Boolean Legacycheck(vo.FtpInfo ftpInfo)
        {
            String deleteFilename = "";
            if (ftpInfo.clip_mov_edit_count > 2)
            {
                // count 하나 감소하고 지우기
                m_strTargetPathWithoutFileName = Path.GetDirectoryName(m_strTargetPath).Replace(@"\", "/");
                m_ftppath = m_strTargetPathWithoutFileName;
                deleteFilename = String.Format("{0}_{1}{2}", Path.GetFileNameWithoutExtension(ftpInfo.old_targetfilename), (ftpInfo.clip_mov_edit_count - 2).ToString("D2"), Path.GetExtension(ftpInfo.old_targetfilename));
            }
            else if (ftpInfo.clip_mov_edit_count == 2)
            {
                //원래 이름 지우기
                deleteFilename = ftpInfo.old_targetfilename;
            }
            else if (ftpInfo.clip_mov_edit_count < 2)
            {
                return false;
            }
            m_deletePath = String.Format("{0}:{1}/{2}/{3}", m_host, m_port, ftpInfo.targetpath, deleteFilename);

            return true;
        }

        public Boolean DeleteFile()
        {
            if (!String.IsNullOrEmpty(m_deletePath))
            {
                try
                {
                    FtpWebRequest requestDelete = WebRequest.Create(m_deletePath) as FtpWebRequest;
                    requestDelete.Credentials = new NetworkCredential(m_id, m_pw);
                    requestDelete.Method = WebRequestMethods.Ftp.DeleteFile;

                    FtpWebResponse ftpResponse = requestDelete.GetResponse() as FtpWebResponse;
                }
                catch
                {
                    return false;
                }
            }
            m_deletePath = "";
            return true;
        }

        public Boolean SendFile(out String errmsg)
        {
            errmsg = "";
            if (String.IsNullOrEmpty(m_strSourcePath) | String.IsNullOrEmpty(m_strTargetPath))
            {
                //src or dst is null
                // ERROR_CODE
                return false;
            }

            if (String.IsNullOrEmpty(m_id) | String.IsNullOrEmpty(m_pw) | String.IsNullOrEmpty(m_pk))
            {
                // ERROR_CODE
                return false;
            }

            frmMain.WriteLogThread("[FTPMgr] make folder target : " + m_ftppath);
            FtpWebRequest requestMkdir = null;
            FtpWebResponse responseMkdir = null;
            String[] subDirs;
            String currenDir;
            currenDir = String.Format(@"{0}:{1}", m_host, m_port);

            subDirs = m_ftppath.Split('/');
            foreach (String subDir in subDirs)
            {
                try
                {
                    currenDir = currenDir + "/" + subDir;
                    //frmMain.WriteLogThread("currDir : " + currenDir);
                    requestMkdir = (FtpWebRequest)WebRequest.Create(currenDir);
                    requestMkdir.Method = WebRequestMethods.Ftp.MakeDirectory;
                    requestMkdir.UsePassive = true;
                    requestMkdir.UseBinary = true;
                    requestMkdir.KeepAlive = false;
                    requestMkdir.Timeout = 5000;
                    requestMkdir.EnableSsl = false;
                    requestMkdir.Credentials = new NetworkCredential(m_id, m_pw);

                    //log.logging("[FTPMgr] Before Mkdir : " + currenDir);
                    responseMkdir = (FtpWebResponse)requestMkdir.GetResponse();
                    frmMain.WriteLogThread("[FTPMgr] Folder Make Successful " + currenDir);
                    log.logging("[FTPMgr] Folder Make Successful : " + responseMkdir.StatusCode.ToString());
                    log.logging("[FTPMgr] code description : " + responseMkdir.StatusDescription.ToString());
                    responseMkdir.Close();
                }
                catch
                {
                    //frmMain.WriteLogThread("[FTPMgr:MakeDir] " + e.ToString());
                    //log.logging(e.ToString());
                    //log.logging(e.StackTrace.ToString());
                    //frmMain.WriteLogThread("[FTPMgr] " + m_ftppath + " is exist");
                    //log.logging("[FTPMgr] " + currenDir + " is exist");
                }
                finally
                {
                    if (responseMkdir != null)
                    {
                        responseMkdir.Close();
                    }
                }
            }

            int failcnt = 0;
            bool isSuccess = true;

            while (failcnt < 3)
            {
                try
                {
                    frmMain.WriteLogThread("[FTPMgr] upload target : " + m_uploadPath);
                    FtpWebRequest requestUpload = (FtpWebRequest)WebRequest.Create(m_uploadPath);
                    requestUpload.Method = WebRequestMethods.Ftp.UploadFile;
                    //패시브 액티브 여부 나중에 정리
                    requestUpload.UsePassive = true;
                    requestUpload.UseBinary = true;
                    requestUpload.KeepAlive = false;
                    requestUpload.Timeout = 100000;
                    // 주의 2017-07-17에 추가됨 (EnableSsl)
                    requestUpload.EnableSsl = false;
                    requestUpload.Credentials = new NetworkCredential(m_id, m_pw);

                    using (var inputStream = File.OpenRead(m_strSourcePath))
                    {
                        frmMain.WriteLogThread("[FTPMgr] ftp source path : " + m_strSourcePath);
                        using (var outputStream = requestUpload.GetRequestStream())
                        {
                            var buffer = new byte[1024 * 1024];
                            ulong totalRealBytesCount = 0;
                            int readBytesCount = 0;
                            double percent = 0;
                            int bufferCnt = 0;
                            long t1 = System.DateTime.Now.Ticks;
                            while ((readBytesCount = inputStream.Read(buffer, 0, buffer.Length)) > 0)
                            {
                                outputStream.Write(buffer, 0, readBytesCount);
                                totalRealBytesCount += (ulong)readBytesCount;
                                percent = totalRealBytesCount * 100.0 / inputStream.Length;
                                //Console.WriteLine("{3} | {0} MB 중 {1} MB 전송 | {2:F}%", inputStream.Length / 1024 / 1024, totalRealBytesCount / 1024 / 1024, percent, program_title);
                                // 10 MB에 한번씩 상태 update
                                if (bufferCnt % 10 == 0 || (ulong)inputStream.Length == totalRealBytesCount)
                                {
                                    // 100MB 올린 시간
                                    long t2 = System.DateTime.Now.Ticks;
                                    mapper.UpdateSendingProgress(m_pk, percent);
                                }
                                bufferCnt++;
                            }
                        }
                    }
                    isSuccess = true;
                    break;
                }
                catch (Exception e)
                {
                    frmMain.WriteLogThread("[FTPMgr] " + e.ToString());
                    log.logging(e.ToString());
                    log.logging(e.StackTrace.ToString());
                    errmsg = e.ToString();
                    failcnt++;
                    log.logging(String.Format("({0}) FailCount : {1}", m_pk, failcnt));
                    isSuccess = false;
                    //return false;
                }
            }
            // 파일 전송 완료
            if (isSuccess == true)
            {
                return true;
            }
            else
            {
                return false;
            }
        }
    }
}