using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading;
using System.Windows.Forms;
using System.Data;
using System.IO;
using MySql.Data;
using MySql.Data.MySqlClient;
using MediaInfoLib;

namespace MBCPLUS_DAEMON
{
    class ArchiveClipService
    {        
        private Boolean _shouldStop = false;
        
        private ConnectionPool connPool;
        private Log log;
        private SqlMapper mapper;
        private String m_sql = "";

        public ArchiveClipService()
        {
            // put this class Name
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

        public MediaInfomation ExtractInformation(String strSourcePath)
        {
            //int duration = 0;            
            MediaInfomation mediainfo = new MediaInfomation();
            try
            {
                frmMain.WriteLogThread("[MediaInfo] Mediainfo Source Path : " + strSourcePath);
                if (Path.GetExtension(strSourcePath).ToLower() != ".jpg" && Path.GetExtension(strSourcePath).ToLower() != ".png")
                {
                    //filesize check

                    FileInfo fInfo = new FileInfo(strSourcePath);
                    mediainfo.filesize = fInfo.Length.ToString();
                    MediaInfo pMi = new MediaInfo();
                    pMi.Open(strSourcePath);

                    mediainfo.duration = pMi.Get(StreamKind.General, 0, "Duration");
                    mediainfo.v_streamkimd = pMi.Get(StreamKind.Video, 0, "StreamKind/String");
                    mediainfo.v_format = pMi.Get(StreamKind.Video, 0, "Format/Info");
                    mediainfo.v_profile = pMi.Get(StreamKind.Video, 0, "Format_Profile");
                    mediainfo.v_version = pMi.Get(StreamKind.Video, 0, "Format_Version");
                    mediainfo.v_gop = pMi.Get(StreamKind.Video, 0, "Format_Settings_GOP");
                    mediainfo.v_cabac = pMi.Get(StreamKind.Video, 0, "Format_Settings_CABAC/String");
                    mediainfo.v_codec = pMi.Get(StreamKind.Video, 0, "Codec");
                    mediainfo.v_bitrate = pMi.Get(StreamKind.Video, 0, "BitRate/String");
                    mediainfo.v_resolution_x = pMi.Get(StreamKind.Video, 0, "Width");
                    mediainfo.v_resolution_y = pMi.Get(StreamKind.Video, 0, "Height");
                    mediainfo.a_codec = pMi.Get(StreamKind.Audio, 0, "Codec");
                    mediainfo.a_bitrate = pMi.Get(StreamKind.Audio, 0, "BitRate/String");
                    pMi.Close();

                    mediainfo.duration = (Convert.ToInt32(mediainfo.duration) / 1000).ToString();
                    /*
                    frmMain.WriteLogThread("[MediaInfo] duration : " + mediainfo.duration);
                    frmMain.WriteLogThread("[MediaInfo] StreamKind : " + mediainfo.v_streamkimd);
                    frmMain.WriteLogThread("[MediaInfo] Format : " + mediainfo.v_format);
                    frmMain.WriteLogThread("[MediaInfo] Profile : " + mediainfo.v_profile);
                    frmMain.WriteLogThread("[MediaInfo] Version : " + mediainfo.v_version);
                    frmMain.WriteLogThread("[MediaInfo] GOP : " + mediainfo.v_gop);
                    frmMain.WriteLogThread("[MediaInfo] CABAC : " + mediainfo.v_cabac);
                    frmMain.WriteLogThread("[MediaInfo] v_codec : " + mediainfo.v_codec);
                    frmMain.WriteLogThread("[MediaInfo] v_bitrate : " + mediainfo.v_bitrate);
                    frmMain.WriteLogThread("[MediaInfo] v_width : " + mediainfo.v_resolution_x);
                    frmMain.WriteLogThread("[MediaInfo] v_height : " + mediainfo.v_resolution_y);
                    frmMain.WriteLogThread("[MediaInfo] a_codec : " + mediainfo.a_codec);
                    frmMain.WriteLogThread("[MediaInfo] a_bitrate : " + mediainfo.a_bitrate);                        
                     */                    
                }
            }
            catch (Exception ex)
            {
                frmMain.WriteLogThread("[MediaInfo] File Load Error : " + strSourcePath);
                frmMain.WriteLogThread("[MediaInfo] " + ex.ToString());
                log.logging(ex.ToString());
                throw;
            }
            return mediainfo;
        }

        void Run()
        {               
            //String status = null;
            MySqlCommand cmd;

            connPool = Singleton.getInstance().GetConnectionPool();
            connPool.SetConnection(new MySqlConnection(Singleton.getInstance().GetStrConn()));
            
            //Waiting for make winform
            Thread.Sleep(10000);
            //frmMain.WriteLogThread("Archive Clip Service Start...");
            log.logging("Service Start...");

            while (!_shouldStop)
            {
                try
                {
                    DataSet ds = new DataSet();
                    ClipInfo clipInfo = new ClipInfo();
                    mapper.GetArchiveClipService(ds);

                    foreach (DataRow r in ds.Tables[0].Rows)
                    {
                        try
                        {
                            clipInfo.clip_pk = r["clip_pk"].ToString();
                            clipInfo.imgsrcpath = r["imgsrcpath"].ToString();
                            clipInfo.clipsrcpath = r["clipsrcpath"].ToString();
                            clipInfo.cdnimg = r["cdnurl_img"].ToString();
                            clipInfo.cdnmov = r["cdnurl_mov"].ToString();
                            clipInfo.orgimgname = r["orgimgname"].ToString();
                            clipInfo.orgclipname = r["orgclipname"].ToString();
                            clipInfo.gid = r["gid"].ToString();
                            clipInfo.cid = r["cid"].ToString();
                            clipInfo.archive_date = r["archive_date"].ToString();
                            clipInfo.metahub_YN = r["metahub_YN"].ToString();
                            clipInfo.section = r["section"].ToString();
                            clipInfo.idolvod_YN = r["idolvod_YN"].ToString();
                            clipInfo.idolclip_YN = r["idolclip_YN"].ToString();
                            clipInfo.idolvote_YN = r["idolvote_YN"].ToString();
                            clipInfo.yt_isuse = r["yt_isuse"].ToString();
                            clipInfo.yt_videoid = r["yt_videoid"].ToString();
                            clipInfo.dm_isuse = r["dm_isuse"].ToString();
                            clipInfo.dm_videoid = r["dm_videoid"].ToString();
                            clipInfo.archive_img = r["archive_img"].ToString();
                            clipInfo.archive_clip = r["archive_clip"].ToString();

                            if (clipInfo.clipsrcpath != "")
                            {
                                clipInfo.clipsrcpath = String.Format(@"Z:{0}", clipInfo.clipsrcpath.Substring(2, clipInfo.clipsrcpath.Length - 2));
                                if ( clipInfo.orgclipname == "")
                                {
                                    clipInfo.orgclipname = Path.GetFileName(clipInfo.clipsrcpath);
                                }
                            }
                            //int playtime = 0;
                            //bool errFlag = false;
                            // clipsrcpath 는 WMS web에서 upload가 된 위치
                            if (!String.IsNullOrEmpty(clipInfo.clipsrcpath) && File.Exists(clipInfo.clipsrcpath))
                            {
                                frmMain.WriteLogThread("[ArchiveService] clip src : " + clipInfo.clipsrcpath);
                                MediaInfomation mediainfo = new MediaInfomation();
                                mediainfo = ExtractInformation(clipInfo.clipsrcpath);
                                m_sql = String.Format(@"UPDATE TB_CLIP SET playtime = '{0}'
                                                    , starttime = CURRENT_TIMESTAMP()
                                                    , v_bitrate = '{1}'
                                                    , v_codec = '{2}'
                                                    , v_resol_x = '{3}'
                                                    , v_resol_y = '{4}'
                                                    , a_bitrate = '{5}'
                                                    , a_codec = '{6}'
                                                    , v_profile = '{8}'
                                                    , v_gop = '{9}'
                                                    , v_cabac = '{10}'
                                                    , v_format = '{11}'
                                                    , v_version = '{12}'
                                                    , filesize = '{13}'
                                                    , status = 'Archive'
                                                    WHERE clip_pk = '{7}'"
                                                    , mediainfo.duration
                                                    , mediainfo.v_bitrate
                                                    , mediainfo.v_codec
                                                    , mediainfo.v_resolution_x
                                                    , mediainfo.v_resolution_y
                                                    , mediainfo.a_bitrate
                                                    , mediainfo.a_codec
                                                    , clipInfo.clip_pk
                                                    , mediainfo.v_profile
                                                    , mediainfo.v_gop
                                                    , mediainfo.v_cabac
                                                    , mediainfo.v_format
                                                    , mediainfo.v_version
                                                    , mediainfo.filesize);
                            }
                            else
                            {
                                m_sql = String.Format("UPDATE TB_CLIP SET starttime = CURRENT_TIMESTAMP(), status = 'Archive' WHERE clip_pk = '{0}'", clipInfo.clip_pk);
                            }

                            connPool.ConnectionOpen();
                            //m_sql = String.Format("UPDATE TB_CLIP SET playtime = '{0}', starttime = CURRENT_TIMESTAMP(), status = 'Archive' WHERE clip_pk = '{1}'", playtime, m_clip_pk);
                            //Running 으로 변경
                            cmd = new MySqlCommand(m_sql, connPool.getConnection());
                            cmd.ExecuteNonQuery();
                            connPool.ConnectionClose();

                            frmMain.WriteLogThread(String.Format(@"[ArchiveService] clip_pk({0}) is Archive", clipInfo.clip_pk));
                            String targetPath = "";

                            StringBuilder sb = new StringBuilder();
                            sb.Append(Util.getSectionPath(clipInfo.section));
                            sb.Append(clipInfo.archive_date);
                            sb.Append(Path.DirectorySeparatorChar);
                            sb.Append(clipInfo.gid);
                            sb.Append(Path.DirectorySeparatorChar);
                            sb.Append(clipInfo.cid);

                            targetPath = sb.ToString();

                            frmMain.WriteLogThread(targetPath);

                            // 등록시 이미지를 안올리면 m_imgsrcpath 가 NULL
                            if (!String.IsNullOrEmpty(clipInfo.imgsrcpath) && File.Exists(clipInfo.imgsrcpath))
                            {
                                try
                                {
                                    if (!Directory.Exists(targetPath))
                                    {
                                        Directory.CreateDirectory(targetPath);
                                    }
                                }
                                catch (Exception e)
                                {
                                    frmMain.WriteLogThread(e.ToString());
                                }
                                connPool.ConnectionOpen();
                                m_sql = String.Format(@"INSERT INTO TB_ARCHIVE(insert_time, clip_pk, srcpath, targetpath, type, status)
                                                VALUES (CURRENT_TIMESTAMP(), '{0}', '{1}', '{2}', 'IMG', 'Pending')", clipInfo.clip_pk
                                                , Util.escapedPath(clipInfo.imgsrcpath)
                                                , Util.escapedPath(targetPath + Path.DirectorySeparatorChar + clipInfo.cid + Path.GetExtension(clipInfo.orgimgname.ToLower())));

                                cmd = new MySqlCommand(m_sql, connPool.getConnection());
                                cmd.ExecuteNonQuery();
                                connPool.ConnectionClose();
                            }
                            else if (clipInfo.cdnimg.Length > 7)
                            {   
                                // 이미 cdn에 이미지가 올라 갔다고 판단 되었을 때
                                connPool.ConnectionOpen();
                                if (clipInfo.metahub_YN == "Y")
                                {
                                    //Ready 로 변경
                                    //수정이 되어서 메타허브로는 반영이 되어야 할 때
                                    m_sql = String.Format("UPDATE TB_CLIP SET status = 'Ready' WHERE clip_pk = '{0}'", clipInfo.clip_pk);
                                }
                                else
                                {
                                    m_sql = String.Format("UPDATE TB_CLIP SET endtime = CURRENT_TIMESTAMP(), status = 'Completed' WHERE clip_pk = '{0}'", clipInfo.clip_pk);
                                }
                                cmd = new MySqlCommand(m_sql, connPool.getConnection());
                                cmd.ExecuteNonQuery();
                                connPool.ConnectionClose();
                                frmMain.WriteLogThread(String.Format(@"[ArchiveService] {0} is already exist, cid = {1}", clipInfo.cdnimg, clipInfo.cid));
                            }
                            else
                            {
                                mapper.UpdateClipStatus(clipInfo.cid, "Completed");
                                if (!String.IsNullOrEmpty(clipInfo.dm_videoid))
                                {
                                    mapper.UpdateDailymotionReady(clipInfo.cid);
                                }
                            }
                            
                            if (!String.IsNullOrEmpty(clipInfo.clipsrcpath) && File.Exists(clipInfo.clipsrcpath))
                            {
                                try
                                {
                                    if (!Directory.Exists(targetPath))
                                    {
                                        Directory.CreateDirectory(targetPath);
                                    }
                                }
                                catch (Exception e)
                                {
                                    frmMain.WriteLogThread(e.ToString());
                                    log.logging(e.ToString());
                                }

                                m_sql = String.Format(@"INSERT INTO TB_ARCHIVE(insert_time, clip_pk, srcpath, targetpath, type, status)
                                                VALUES (CURRENT_TIMESTAMP(), '{0}', '{1}', '{2}', 'MOV', 'Pending')", clipInfo.clip_pk
                                                , Util.escapedPath(clipInfo.clipsrcpath)
                                                , Util.escapedPath(targetPath + Path.DirectorySeparatorChar + clipInfo.cid + Path.GetExtension(clipInfo.orgclipname.ToLower())));

                                connPool.ConnectionOpen();
                                cmd = new MySqlCommand(m_sql, connPool.getConnection());
                                cmd.ExecuteNonQuery();
                                connPool.ConnectionClose();
                            }
                            else if (clipInfo.cdnmov.Length > 7)
                            {
                                // CDN에 영상이 이미 있는 경우                                
                                frmMain.WriteLogThread(String.Format("cid : {0}, yt_isuse : {1}, dm_isuse : {2}", clipInfo.cid, clipInfo.yt_isuse, clipInfo.dm_isuse));
                                if (clipInfo.idolvod_YN == "Y" || clipInfo.idolclip_YN == "Y" || clipInfo.idolvote_YN == "Y" 
                                    || (clipInfo.yt_isuse != "T" && !String.IsNullOrEmpty(clipInfo.yt_isuse))
                                    || (clipInfo.dm_isuse != "T" && !String.IsNullOrEmpty(clipInfo.dm_isuse)) )
                                {
                                    String srcPath = null;
                                    if (clipInfo.yt_isuse != "T" && !String.IsNullOrEmpty(clipInfo.yt_isuse))
                                    {
                                        if (String.IsNullOrEmpty(clipInfo.yt_videoid))
                                        {
                                            mapper.SetAdditionalCustomer(clipInfo, "9", "IMG");
                                            mapper.SetAdditionalCustomer(clipInfo, "9", "MOV");
                                        }
                                    }
                                    if (clipInfo.dm_isuse != "T" && !String.IsNullOrEmpty(clipInfo.dm_isuse))
                                    {
                                        if (String.IsNullOrEmpty(clipInfo.dm_videoid))
                                        {
                                            mapper.SetAdditionalCustomer(clipInfo, "10", "MOV");
                                        }
                                        else
                                        {
                                            mapper.UpdateDailymotionReady(clipInfo.cid);
                                        }
                                    }
                                    // 셋중에 하나라도 Y일경우 check
                                    if ( String.IsNullOrEmpty(clipInfo.clipsrcpath) && mapper.GetIdolChampCheck(clipInfo.cid))
                                    {
                                        
                                    }
                                    else
                                    {
                                        //추가 트랜스코딩을 요청해야함 (어떤 데이터로??)                                        
                                        if (mapper.SetAdditionalTranscoding(clipInfo.clip_pk, clipInfo.gid, clipInfo.cid, out srcPath))
                                        {
                                            // 실행 완료                                            
                                        }
                                        else
                                        {
                                            // 트랜젝션 롤백
                                            // srcPath = null
                                        }
                                    }
                                }
                                // 이미 트랜스코딩이 된경우
                                if (clipInfo.metahub_YN == "Y")
                                {
                                    m_sql = String.Format("UPDATE TB_CLIP SET status = 'Ready' WHERE clip_pk = '{0}'", clipInfo.clip_pk);
                                }
                                else
                                {
                                    m_sql = String.Format("UPDATE TB_CLIP SET endtime = CURRENT_TIMESTAMP(), status = 'Completed' WHERE clip_pk = '{0}'", clipInfo.clip_pk);
                                }
                                //XML은 재전송 될 수 있게 Pending으로 변경
                                connPool.ConnectionOpen();
                                cmd = new MySqlCommand(m_sql, connPool.getConnection());
                                cmd.ExecuteNonQuery();
                                connPool.ConnectionClose();
                                frmMain.WriteLogThread(String.Format(@"[ArchiveService] {0} is already exist, cid = {1}", clipInfo.cdnmov, clipInfo.cid));

                                connPool.ConnectionOpen();
                                m_sql = String.Format(@"UPDATE TB_FTP_QUEUE
                                                SET starttime = CURRENT_TIMESTAMP()
                                                , status = 'Pending'
                                                WHERE clip_pk = '{0}'
                                                AND type = 'XML'", clipInfo.clip_pk);
                                cmd = new MySqlCommand(m_sql, connPool.getConnection());
                                cmd.ExecuteNonQuery();
                                connPool.ConnectionClose();
                                frmMain.WriteLogThread(String.Format(@"[ArchiveService] clip_pk({0}) XML changed Pending", clipInfo.clip_pk));
                            }
                            else
                            {
                                // CDN에 업로드 하지 않고 추가로 영상 업로드도 하지 않았을 때 수정 목적
                                mapper.UpdateClipStatus(clipInfo.cid, "Completed");
                            }
                        }
                        catch (Exception e)
                        {
                            log.logging(e.ToString());
                            frmMain.WriteLogThread("[ArchiveService] " + e.ToString());

                            m_sql = String.Format("UPDATE TB_CLIP SET endtime = CURRENT_TIMESTAMP(), status = 'Failed' WHERE clip_pk = '{0}'", clipInfo.clip_pk);

                            connPool.ConnectionOpen();
                            cmd = new MySqlCommand(m_sql, connPool.getConnection());
                            cmd.ExecuteNonQuery();
                            connPool.ConnectionClose();
                        }
                    }
                    ds.Clear();
                }
                catch (Exception e)
                {
                    frmMain.WriteLogThread(e.ToString());
                    log.logging(e.ToString());
                }
                Thread.Sleep(1000);
            }
            connPool.ConnectionDisPose();
            log.logging("Thread Terminate");
        }
    }
}
