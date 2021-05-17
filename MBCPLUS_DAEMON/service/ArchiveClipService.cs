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
    internal class ArchiveClipService
    {
        private Boolean _shouldStop = false;

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

        private void DoWork()
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

        private void Run()
        {
            //String status = null;
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
                            clipInfo.subtitlesrcpath = r["subtitlesrcpath"].ToString();
                            clipInfo.cdnimg = r["cdnurl_img"].ToString();
                            clipInfo.cdnmov = r["cdnurl_mov"].ToString();
                            clipInfo.cdnsubtitle = r["cdnurl_subtitle"].ToString();
                            clipInfo.orgimgname = r["orgimgname"].ToString();
                            clipInfo.orgclipname = r["orgclipname"].ToString();
                            clipInfo.orgsubtitlename = r["orgsubtitlename"].ToString();
                            clipInfo.gid = r["gid"].ToString();
                            clipInfo.cid = r["cid"].ToString();
                            //new
                            clipInfo.broaddate = r["broaddate"].ToString();
                            //clipInfo.archive_date = r["archive_date"].ToString();
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
                            clipInfo.archive_subtitle = r["archive_subtitle"].ToString();
                            clipInfo.isuse = r["isuse"].ToString();
                            clipInfo.edit_img_count = Convert.ToInt32(r["edit_img_count"].ToString());
                            clipInfo.edit_clip_count = Convert.ToInt32(r["edit_clip_count"].ToString());
                            clipInfo.edit_vod_img_count = Convert.ToInt32(r["edit_vod_img_count"].ToString());
                            clipInfo.edit_vod_clip_count = Convert.ToInt32(r["edit_vod_clip_count"].ToString());
                            clipInfo.isvod = r["isvod"].ToString();

                            clipInfo.yt_upload_img = r["yt_upload_img"].ToString();
                            clipInfo.yt_upload_srt = r["yt_upload_srt"].ToString();
                            clipInfo.yt_org_img = r["yt_org_img"].ToString();
                            clipInfo.yt_org_srt = r["yt_org_srt"].ToString();
                            clipInfo.yt_cdn_img = r["yt_cdn_img"].ToString();
                            clipInfo.yt_cdn_srt = r["yt_cdn_srt"].ToString();
                            clipInfo.yt_ar_img = r["yt_ar_img"].ToString();
                            clipInfo.yt_ar_srt = r["yt_ar_srt"].ToString();

                            clipInfo.yt_type = r["yt_type"].ToString();

                            //상태를 Archive로 바꿈
                            mapper.UpdateClipStatus(clipInfo.cid, "Archiving");

                            if (clipInfo.clipsrcpath != "")
                            {
                                clipInfo.clipsrcpath = String.Format(@"Z:{0}", clipInfo.clipsrcpath.Substring(2, clipInfo.clipsrcpath.Length - 2));
                                if (clipInfo.orgclipname == "")
                                {
                                    clipInfo.orgclipname = Path.GetFileName(clipInfo.clipsrcpath);
                                }
                            }
                            // clipsrcpath 는 WMS web에서 upload가 된 위치
                            if (!String.IsNullOrEmpty(clipInfo.clipsrcpath) && File.Exists(clipInfo.clipsrcpath))
                            {
                                frmMain.WriteLogThread("[ArchiveService] clip src : " + clipInfo.clipsrcpath);
                                MediaInfomation mediainfo = new MediaInfomation();
                                mediainfo = ExtractInformation(clipInfo.clipsrcpath);
                                mapper.UpdateClipInfos(clipInfo.cid, mediainfo);
                            }

                            //log.logging(String.Format(@"[ArchiveService] cid ({0}) is Archived", clipInfo.cid));

                            String targetPath = "";
                            string dateFromCID = clipInfo.cid.Substring(2, 8);
                            dateFromCID = string.Format($"{dateFromCID.Substring(0, 4)}\\{dateFromCID.Substring(4, 2)}\\{dateFromCID.Substring(6, 2)}");

                            StringBuilder sb = new StringBuilder();
                            if (Singleton.getInstance().Test)
                            {
                                sb.Append(Util.getTestPath());
                            }
                            else
                            {
                                sb.Append(Util.getSectionPath(clipInfo.section));
                            }
                            sb.Append(dateFromCID);
                            sb.Append(Path.DirectorySeparatorChar);
                            sb.Append(clipInfo.gid);
                            sb.Append(Path.DirectorySeparatorChar);
                            sb.Append(clipInfo.cid);

                            targetPath = sb.ToString();

                            frmMain.WriteLogThread(targetPath);

                            // 등록시 이미지를 안올리면 m_imgsrcpath 가 NULL
                            // 이미지
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
                                    //frmMain.WriteLogThread(e.ToString());
                                    log.logging(e.ToString());
                                }

                                String edit_count_string = "";
                                String dstpath = "";

                                if (clipInfo.isvod == "Y")
                                {
                                    //if (ftpInfo.clip_mov_edit_count > 1 && ftpInfo.type.ToLower() == "mov")
                                    if (clipInfo.edit_vod_img_count > 1)
                                    {
                                        edit_count_string = String.Format("_{0}", (clipInfo.edit_vod_img_count - 1).ToString("D2"));
                                        //ftpInfo.targetfilename = String.Format("{0}{1}{2}", Path.GetFileNameWithoutExtension(ftpInfo.targetfilename), edit_count_title, Path.GetExtension(ftpInfo.targetfilename));
                                    }
                                }
                                else
                                {
                                    if (clipInfo.edit_img_count > 1)
                                    {
                                        edit_count_string = String.Format("_{0}", (clipInfo.edit_img_count - 1).ToString("D2"));
                                    }
                                }
                                dstpath = Util.escapedPath(targetPath + Path.DirectorySeparatorChar + clipInfo.cid + edit_count_string + Path.GetExtension(clipInfo.orgimgname.ToLower()));
                                mapper.ArchiveClip(clipInfo.clip_pk
                                    , Util.escapedPath(clipInfo.imgsrcpath)
                                    , dstpath
                                    , "IMG", clipInfo.cid
                                    , edit_count_string);
                            }
                            else if (clipInfo.cdnimg.Length > 7)
                            {
                                // 이미 cdn에 이미지가 올라 갔다고 판단 되었을 때
                                if (!String.IsNullOrEmpty(clipInfo.clipsrcpath) && File.Exists(clipInfo.clipsrcpath))
                                {
                                    /*
                                    connPool.ConnectionOpen();
                                    m_sql = String.Format("UPDATE TB_CLIP SET endtime = CURRENT_TIMESTAMP(), status = 'Completed' WHERE clip_pk = '{0}'", clipInfo.clip_pk);
                                    cmd = new MySqlCommand(m_sql, connPool.getConnection());
                                    cmd.ExecuteNonQuery();
                                    connPool.ConnectionClose();
                                    */
                                    mapper.UpdateClipStatus(clipInfo.cid, "Completed");
                                }
                                frmMain.WriteLogThread(String.Format(@"[ArchiveService] cdnimg : {0} is already exist, cid = {1}", clipInfo.cdnimg, clipInfo.cid));

                                /*
                                // cdn에 있더라도 아카이브 된 이미지를 전송함
                                String archive_srcpath = clipInfo.archive_img;
                                archive_srcpath = String.Format(@"Z:\{0}", archive_srcpath);
                                archive_srcpath = archive_srcpath.Replace("/", @"\");
                                connPool.ConnectionOpen();
                                m_sql = String.Format(@"INSERT INTO TB_ARCHIVE(insert_time, clip_pk, srcpath, targetpath, type, status, cid)
                                                VALUES (CURRENT_TIMESTAMP(), '{0}', '{1}', '{2}', 'IMG', 'Pending', '{3}')", clipInfo.clip_pk
                                                , archive_srcpath
                                                , Util.escapedPath(targetPath + Path.DirectorySeparatorChar + clipInfo.cid + Path.GetExtension(clipInfo.orgimgname.ToLower()))
                                                , clipInfo.cid
                                                );

                                cmd = new MySqlCommand(m_sql, connPool.getConnection());
                                cmd.ExecuteNonQuery();
                                connPool.ConnectionClose();
                                */
                            }

                            // 유튜브 이미지 아카이브
                            if (!String.IsNullOrEmpty(clipInfo.yt_upload_img) && File.Exists(clipInfo.yt_upload_img))
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
                                    //frmMain.WriteLogThread(e.ToString());
                                    log.logging(e.ToString());
                                }
                                String edit_count_string = "";
                                mapper.ArchiveClip(clipInfo.clip_pk
                                    , Util.escapedPath(clipInfo.yt_upload_img)
                                    , Util.escapedPath(targetPath + Path.DirectorySeparatorChar + clipInfo.cid + "_YT1" + Path.GetExtension(clipInfo.yt_org_img.ToLower()))
                                    , "YT_IMG"
                                    , clipInfo.cid
                                    , edit_count_string);
                            }

                            //자막
                            if (!String.IsNullOrEmpty(clipInfo.subtitlesrcpath) && File.Exists(clipInfo.subtitlesrcpath))
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
                                    //frmMain.WriteLogThread(e.ToString());
                                    log.logging(e.ToString());
                                }
                                String edit_string_count = "";
                                mapper.ArchiveClip(clipInfo.clip_pk
                                    , Util.escapedPath(clipInfo.subtitlesrcpath)
                                    , Util.escapedPath(targetPath + Path.DirectorySeparatorChar + clipInfo.cid + Path.GetExtension(clipInfo.subtitlesrcpath.ToLower()))
                                    , "SRT"
                                    , clipInfo.cid
                                    , edit_string_count);

                                /*
                                connPool.ConnectionOpen();
                                m_sql = String.Format(@"INSERT INTO TB_ARCHIVE(insert_time, clip_pk, srcpath, targetpath, type, status, cid)
                                                VALUES (CURRENT_TIMESTAMP(), '{0}', '{1}', '{2}', 'SRT', 'Pending', '{3}')", clipInfo.clip_pk
                                                , Util.escapedPath(clipInfo.subtitlesrcpath)
                                                , Util.escapedPath(targetPath + Path.DirectorySeparatorChar + clipInfo.cid + Path.GetExtension(clipInfo.subtitlesrcpath.ToLower()))
                                                , clipInfo.cid
                                                );
                                cmd = new MySqlCommand(m_sql, connPool.getConnection());
                                cmd.ExecuteNonQuery();
                                connPool.ConnectionClose();
                                */
                            }

                            //유튜브 자막
                            if (!String.IsNullOrEmpty(clipInfo.yt_upload_srt) && File.Exists(clipInfo.yt_upload_srt))
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
                                    //frmMain.WriteLogThread(e.ToString());
                                    log.logging(e.ToString());
                                }
                                String edit_count_string = "";
                                mapper.ArchiveClip(clipInfo.clip_pk
                                    , Util.escapedPath(clipInfo.yt_upload_srt)
                                    , Util.escapedPath(targetPath + Path.DirectorySeparatorChar + clipInfo.cid + "_YT1" + Path.GetExtension(clipInfo.yt_org_srt.ToLower()))
                                    , "YT_SRT"
                                    , clipInfo.cid
                                    , edit_count_string);
                                /*
                                connPool.ConnectionOpen();
                                m_sql = String.Format(@"INSERT INTO TB_ARCHIVE(insert_time, clip_pk, srcpath, targetpath, type, status, cid)
                                                VALUES (CURRENT_TIMESTAMP(), '{0}', '{1}', '{2}', 'SRT', 'Pending', '{3}')", clipInfo.clip_pk
                                                , Util.escapedPath(clipInfo.subtitlesrcpath)
                                                , Util.escapedPath(targetPath + Path.DirectorySeparatorChar + clipInfo.cid + Path.GetExtension(clipInfo.subtitlesrcpath.ToLower()))
                                                , clipInfo.cid
                                                );
                                cmd = new MySqlCommand(m_sql, connPool.getConnection());
                                cmd.ExecuteNonQuery();
                                connPool.ConnectionClose();
                                */
                            }

                            //유튜브 아이템
                            DataSet ds_item = mapper.GetYoutubeITEMS(clipInfo.cid);
                            foreach (DataRow r_item in ds_item.Tables[0].Rows)
                            {
                                vo.YtItems ytItem = new vo.YtItems();
                                ytItem.cid = clipInfo.cid;
                                ytItem.language = r_item["language"].ToString();
                                ytItem.type = r_item["type"].ToString();
                                ytItem.org_name = r_item["org_name"].ToString();
                                ytItem.upload_path = r_item["upload_path"].ToString();

                                ytItem.upload_path = ytItem.upload_path.Replace("/uploads/", "Z:\\upload_cache\\");

                                String suffix = String.Format("{0}_{1}_{2}", "YT", ytItem.type, ytItem.language.ToUpper());

                                if (!String.IsNullOrEmpty(ytItem.upload_path) && File.Exists(ytItem.upload_path))
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
                                        //frmMain.WriteLogThread(e.ToString());
                                        log.logging(e.ToString());
                                    }
                                    String edit_count_string = "";
                                    mapper.ArchiveClip(clipInfo.clip_pk
                                        , Util.escapedPath(ytItem.upload_path)
                                        , Util.escapedPath(targetPath + Path.DirectorySeparatorChar + ytItem.cid + "_" + suffix + Path.GetExtension(ytItem.upload_path.ToLower()))
                                        , suffix
                                        , ytItem.cid
                                        , edit_count_string);
                                }
                            }

                            //클립 업로드
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

                                String edit_count_string = "";
                                String dstpath = "";

                                if (clipInfo.isvod == "Y")
                                {
                                    //if (ftpInfo.clip_mov_edit_count > 1 && ftpInfo.type.ToLower() == "mov")
                                    if (clipInfo.edit_vod_clip_count > 1)
                                    {
                                        edit_count_string = String.Format("_{0}", (clipInfo.edit_vod_clip_count - 1).ToString("D2"));
                                        //ftpInfo.targetfilename = String.Format("{0}{1}{2}", Path.GetFileNameWithoutExtension(ftpInfo.targetfilename), edit_count_title, Path.GetExtension(ftpInfo.targetfilename));
                                    }
                                }
                                else
                                {
                                    if (clipInfo.edit_clip_count > 1)
                                    {
                                        edit_count_string = String.Format("_{0}", (clipInfo.edit_clip_count - 1).ToString("D2"));
                                    }
                                }
                                dstpath = Util.escapedPath(targetPath + Path.DirectorySeparatorChar + clipInfo.cid + edit_count_string + Path.GetExtension(clipInfo.orgclipname.ToLower()));

                                mapper.ArchiveClip(clipInfo.clip_pk
                                    , Util.escapedPath(clipInfo.clipsrcpath)
                                    , dstpath
                                    , "MOV"
                                    , clipInfo.cid
                                    , edit_count_string);
                            }
                            else if (clipInfo.cdnmov.Length > 7)
                            {
                                // CDN에 영상이 이미 있는 경우
                                frmMain.WriteLogThread(String.Format("cid : {0}, yt_isuse : {1}, dm_isuse : {2}, idolvod : {3}, idolclip : {4}, idolvote : {5}", clipInfo.cid, clipInfo.yt_isuse, clipInfo.dm_isuse, clipInfo.idolvod_YN, clipInfo.idolclip_YN, clipInfo.idolvote_YN));
                                if (clipInfo.idolvod_YN == "Y" || clipInfo.idolclip_YN == "Y" || clipInfo.idolvote_YN == "Y")
                                {
                                    String srcPath = null;
                                    // 셋중에 하나라도 Y일경우 check
                                    if (String.IsNullOrEmpty(clipInfo.clipsrcpath) && mapper.GetIdolChampCheck(clipInfo.cid))
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
                                        log.logging(String.Format("SetAdditionalTranscoding : {0}, srcPath : {1}", clipInfo.cid, srcPath));
                                    }
                                }
                                // 이미 트랜스코딩이 된경우
                                /*
                                if (clipInfo.isuse != "T")
                                {
                                    m_sql = String.Format("UPDATE TB_CLIP SET status = 'Ready' WHERE clip_pk = '{0}'", clipInfo.clip_pk);
                                }
                                else
                                {
                                    m_sql = String.Format("UPDATE TB_CLIP SET endtime = CURRENT_TIMESTAMP(), status = 'Completed' WHERE clip_pk = '{0}'", clipInfo.clip_pk);
                                }*/

                                mapper.UpdateClipStatus(clipInfo.cid, "Completed");

                                frmMain.WriteLogThread(String.Format(@"[ArchiveService] cdnmov : {0} is already exist, cid = {1}", clipInfo.cdnmov, clipInfo.cid));

                                //XML은 재전송 될 수 있게 Pending으로 변경

                                mapper.UpdateFTPXmlPending(clipInfo.clip_pk);

                                frmMain.WriteLogThread(String.Format(@"[ArchiveService] clip_pk({0}) XML changed Pending", clipInfo.clip_pk));
                            }
                            else
                            {
                                // CDN에 업로드 하지 않고 추가로 영상 업로드도 하지 않았을 때 수정 목적
                                mapper.UpdateClipStatus(clipInfo.cid, "Completed");
                            }
                            //youtube & dailymotion 일 경우
                            // 아카이브가 되어있음
                            if (!String.IsNullOrEmpty(clipInfo.archive_clip))
                            {
                                //T ?? isuse가 NULL이 아니라는게 무슨뜻???
                                if (clipInfo.yt_isuse != "T" && !String.IsNullOrEmpty(clipInfo.yt_isuse) && clipInfo.yt_type == "WMS")
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
                            }

                            frmMain.WriteLogThread(String.Format("clipInfo.imgsrcpath : {0}, clipInfo.clipsrcpath : {1}", clipInfo.imgsrcpath, clipInfo.clipsrcpath));
                            if (String.IsNullOrEmpty(clipInfo.imgsrcpath) && String.IsNullOrEmpty(clipInfo.clipsrcpath))
                            {
                                frmMain.WriteLogThread(String.Format("yt_videoid : {0}, dm_videoid : {1} is ready", clipInfo.yt_videoid, clipInfo.dm_videoid));
                                // 이미지와 영상을 안올리면 바로 수정
                                if (!String.IsNullOrEmpty(clipInfo.dm_videoid))
                                {
                                    mapper.UpdateDailymotionReady(clipInfo.cid);
                                }
                                if (!String.IsNullOrEmpty(clipInfo.yt_videoid))
                                {
                                    mapper.UpdateYoutubeReady(clipInfo.cid);
                                }
                            }
                        }
                        catch (Exception e)
                        {
                            log.logging(e.ToString());
                            frmMain.WriteLogThread("[ArchiveService] " + e.ToString());

                            mapper.UpdateClipStatus(clipInfo.cid, "Failed", e.ToString());
                            /*
                            m_sql = String.Format("UPDATE TB_CLIP SET endtime = CURRENT_TIMESTAMP(), status = 'Failed' WHERE clip_pk = '{0}'", clipInfo.clip_pk);

                            connPool.ConnectionOpen();
                            cmd = new MySqlCommand(m_sql, connPool.getConnection());
                            cmd.ExecuteNonQuery();
                            connPool.ConnectionClose();
                            */
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

            log.logging("Thread Terminate");
        }
    }
}