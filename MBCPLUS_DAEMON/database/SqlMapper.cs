using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using System.IO;
using MySql.Data;
using MySql.Data.MySqlClient;

namespace MBCPLUS_DAEMON
{
    class SqlMapper
    {
        private String strConn;
        //private String m_gid;
        ConnectionPool connPool;
        String m_sql;
        MySqlCommand cmd;
        Log logger;

        public SqlMapper()
        {
            strConn = Singleton.getInstance().GetStrConn();
            connPool = new ConnectionPool();
            connPool.SetConnection(new MySqlConnection(strConn));             
            logger = new Log(this.GetType().Name);
        }

        public bool InitDataBase(String strTarget)
        {            
            return true;
        }

        public bool SetDMPlayList(String id, String name, int ordernum)
        {
            connPool.ConnectionOpen();
            m_sql = String.Format(@"INSERT INTO TB_DM_PLAYLIST(id, name, ordernum)
                                                  VALUES ('{0}', @name, {1})
                                                  ON DUPLICATE KEY UPDATE id = '{0}', name = @name, ordernum = {1}"
                ,id, ordernum);

            cmd = new MySqlCommand(m_sql, connPool.getConnection());
            cmd.Parameters.Add(new MySqlParameter("@name", name));
            cmd.ExecuteNonQuery();
            connPool.ConnectionClose();
            return true;
        }

        public bool DeleteYTPlayList()
        {
            connPool.ConnectionOpen();
            m_sql = String.Format(@"DELETE FROM TB_YT_PLAYLIST");
            cmd = new MySqlCommand(m_sql, connPool.getConnection());
            cmd.ExecuteNonQuery();
            connPool.ConnectionClose();
            return true;
        }

        public bool SETYTPlayList(String id, String name, String channel_id, int ordernum)
        {
            connPool.ConnectionOpen();
            m_sql = String.Format(@"INSERT INTO TB_YT_PLAYLIST(id, name, order_num, channel_id)
                                                  VALUES ('{0}', @name, {1}, '{2}')
                                                  ON DUPLICATE KEY UPDATE id = '{0}', name = @name, order_num = {1}, channel_id = '{2}'"
                , id, ordernum, channel_id);

            cmd = new MySqlCommand(m_sql, connPool.getConnection());
            cmd.Parameters.Add(new MySqlParameter("@name", name));
            cmd.ExecuteNonQuery();
            connPool.ConnectionClose();
            return true;
        }        

        public bool SETYTChannelList(List<Dictionary<String, String>> list)
        {
            for (int i = 0; i < list.Count; i++)
            {
                connPool.ConnectionOpen();
                m_sql = String.Format(@"INSERT INTO TB_YT_CHANNELLIST(id, name)
                                                  VALUES ('{0}', @name)
                                                  ON DUPLICATE KEY UPDATE id = '{0}', name = @name"
                    , list[i]["id"]);

                cmd = new MySqlCommand(m_sql, connPool.getConnection());
                cmd.Parameters.Add(new MySqlParameter("@name", list[i]["name"]));
                cmd.ExecuteNonQuery();
                connPool.ConnectionClose();
            }
            return true;
        }

        public bool GetYTChannelList(DataSet ds)
        {
            connPool.ConnectionOpen();
            m_sql = String.Format(@"SELECT id, name
                                    FROM TB_YT_CHANNELLIST
                                    ORDER BY order_num ASC
                                    ");            
            MySqlDataAdapter adpt = new MySqlDataAdapter(m_sql, connPool.getConnection());
            adpt.Fill(ds, "YTC");            
            connPool.ConnectionClose();
            return true;
        }

        public bool DeleteYTChannelList()
        {
            connPool.ConnectionOpen();
            m_sql = String.Format(@"DELETE FROM TB_YT_CHANNELLIST");
            cmd = new MySqlCommand(m_sql, connPool.getConnection());
            cmd.ExecuteNonQuery();
            connPool.ConnectionClose();
            return true;
        }

        public bool GetYTAccountList(DataSet ds)
        {
            connPool.ConnectionOpen();
            m_sql = String.Format(@"SELECT channel_id, name, keyfile
                                    FROM TB_YT_ACCOUNT                                    
                                    ORDER BY order_num ASC
                                    ");
            MySqlDataAdapter adpt = new MySqlDataAdapter(m_sql, connPool.getConnection());
            adpt.Fill(ds, "YTA");
            connPool.ConnectionClose();
            return true;
        }

        public bool GetYTAccountList(DataSet ds, String channel_id)
        {
            connPool.ConnectionOpen();
            m_sql = String.Format(@"SELECT channel_id, name, keyfile
                                    FROM TB_YT_ACCOUNT
                                    WHERE channel_id = '{0}'
                                    ", channel_id);
            MySqlDataAdapter adpt = new MySqlDataAdapter(m_sql, connPool.getConnection());
            adpt.Fill(ds, "YTA");
            connPool.ConnectionClose();
            return true;
        }

        public bool WaitYoutubeReady(DataSet ds)
        {
            // Published at 형식 2018-02-23T02:12:17.000Z
            connPool.ConnectionOpen();
            m_sql = String.Format(@"SELECT
                                    YT.cid
                                    , YT.videoid
                                    , YT.channel_id
                                    , YT.title
                                    , YT.description
                                    , YT.information as information
                                    , YT.tag
                                    , YT.category
                                    , YT.status
                                    , YT.spoken_language
                                    , YT.target_language
                                    , YT.org_lang_title
                                    , YT.org_lang_desc
                                    , YT.trans_lang_title
                                    , YT.trans_lang_desc
                                    , YT.session_id
                                    , YT.isuse as privacy
                                    , YT.playlist_id as playlist_id
                                    , YT.old_playlist_id as old_playlist_id
                                    , DATE_FORMAT(DATE_ADD(YT.start_time,INTERVAL -9 HOUR), '%Y-%m-%dT%H:%i:%s.000Z') AS start_time
                                    , C.archive_img as thumbnail
                                    , C.archive_subtitle as caption
                                    FROM TB_YOUTUBE YT                                    
                                    LEFT JOIN TB_CLIP C ON C.cid = YT.cid
                                    WHERE 1=1
                                    AND YT.status = 'Ready'
                                    ");
            //2018-02-23T06:58:21.000Z
            //MySqlTransaction trans = connPool.getConnection().BeginTransaction();
            MySqlDataAdapter adpt = new MySqlDataAdapter(m_sql, connPool.getConnection());
            adpt.Fill(ds, "YT_RDY");
            //trans.Commit();
            connPool.ConnectionClose();
            return true;
        }

        public bool YoutubeCheckInterFace(DataSet ds)
        {
            // Published at 형식 2018-02-23T02:12:17.000Z
            connPool.ConnectionOpen();
            m_sql = String.Format(@"SELECT
                                    YIF.status                                     
                                    FROM TB_YT_INTERFACE YIF
                                    WHERE 1=1
                                    AND name = 'channelNplaylist'
                                    AND YIF.status = 'UpdateList'
                                    ");            
            MySqlDataAdapter adpt = new MySqlDataAdapter(m_sql, connPool.getConnection());
            adpt.Fill(ds, "YT_IF");            
            connPool.ConnectionClose();            
            if (ds.Tables[0].Rows.Count > 0)
            {
                ds.Clear();
                connPool.ConnectionOpen();
                m_sql = String.Format("UPDATE TB_YT_INTERFACE SET status = 'Prepare' WHERE name = 'channelNplaylist'");                
                cmd = new MySqlCommand(m_sql, connPool.getConnection());
                cmd.ExecuteNonQuery();
                connPool.ConnectionClose();
                return true;
            }
            ds.Clear();
            return false;            
        }

        public bool YoutubeRequest(DataSet ds)
        {
            // Published at 형식 2018-02-23T02:12:17.000Z
            connPool.ConnectionOpen();
            m_sql = String.Format(@"SELECT
                                    YT.cid
                                    , YT.videoid
                                    , YT.channel_id
                                    , YT.title
                                    , YT.description
                                    , YT.tag
                                    , YT.category
                                    , YT.status
                                    , YT.spoken_language
                                    , YT.target_language
                                    , YT.org_lang_title
                                    , YT.org_lang_desc
                                    , YT.trans_lang_title
                                    , YT.trans_lang_desc
                                    , YT.session_id
                                    , YT.isuse as privacy
                                    , YT.playlist_id as playlist_id
                                    , DATE_FORMAT(DATE_ADD(YT.start_time,INTERVAL -9 HOUR), '%Y-%m-%dT%H:%i:%s.000Z') AS start_time
                                    , C.archive_img as thumbnail
                                    , C.archive_subtitle as caption
                                    FROM TB_YOUTUBE YT                                    
                                    LEFT JOIN TB_CLIP C ON C.cid = YT.cid
                                    WHERE 1=1
                                    AND YT.status = 'Sync'
                                    ");
            //2018-02-23T06:58:21.000Z
            //MySqlTransaction trans = connPool.getConnection().BeginTransaction();
            MySqlDataAdapter adpt = new MySqlDataAdapter(m_sql, connPool.getConnection());
            adpt.Fill(ds, "YT_REQ");
            //trans.Commit();
            connPool.ConnectionClose();
            return true;
        }

        public bool YoutubePendingCheck(DataSet ds)
        {            
            connPool.ConnectionOpen();
            m_sql = String.Format(@"SELECT
                                    YT.cid
                                    , YT.videoid
                                    , C.archive_img as srcimg
                                    , C.clip_pk as clip_pk
                                    , YT.subtitlepath as srcsubtitle
                                    , C.archive_clip as srcmov
                                    , C.gid as gid
                                    FROM TB_YOUTUBE YT                                    
                                    LEFT JOIN TB_CLIP C ON C.cid = YT.cid
                                    WHERE 1=1
                                    AND YT.status = 'Pending'
                                    ");
            MySqlDataAdapter adpt = new MySqlDataAdapter(m_sql, connPool.getConnection());
            adpt.Fill(ds, "YT_Pending");
            //trans.Commit();
            connPool.ConnectionClose();
            return true;
        }

        public bool DailymotionPendingCheck(DataSet ds)
        {
            connPool.ConnectionOpen();
            m_sql = String.Format(@"SELECT
                                    DM.cid
                                    , DM.videoid
                                    , C.archive_img as srcimg
                                    , DM.subtitlepath as srcsubtitle
                                    , C.archive_clip as srcmov
                                    FROM TB_DAILYMOTION DM
                                    LEFT JOIN TB_CLIP C ON C.cid = DM.cid
                                    WHERE 1=1
                                    AND DM.status = 'Pending'
                                    ");
            MySqlDataAdapter adpt = new MySqlDataAdapter(m_sql, connPool.getConnection());
            adpt.Fill(ds, "DM_Pending");
            //trans.Commit();
            connPool.ConnectionClose();
            return true;
        }

        public bool GetDMReady(DataSet ds)
        {
            connPool.ConnectionOpen();
            m_sql = String.Format(@"SELECT DM.cid as cid
                                    , DM.videoid
                                    , DM.playlistid as playlistid
                                    , DM.old_playlistid as old_playlistid
                                    , DM.title as title
                                    , DM.description
                                    , DM.category
                                    , DM.tag
                                    , DM.isuse
                                    , UNIX_TIMESTAMP(DM.publish_date) as publish_date
                                    , UNIX_TIMESTAMP(DM.expiry_date) as expiry_date
                                    , DM.policy_YN
                                    , DM.geoblock_code
                                    , DM.geoblock_value
                                    , DM.explicit_YN
                                    , C.cdnurl_img as thumbnail_url
                                    , YT.status as yt_status
                                    FROM TB_DAILYMOTION DM
                                    LEFT JOIN TB_CLIP C ON C.cid = DM.cid
                                    LEFT JOIN TB_YOUTUBE YT ON YT.cid = DM.cid
                                    WHERE 1=1
                                    AND DM.status = 'Ready'
                                    ");
            //MySqlTransaction trans = connPool.getConnection().BeginTransaction();
            MySqlDataAdapter adpt = new MySqlDataAdapter(m_sql, connPool.getConnection());
            adpt.Fill(ds, "DM_READY");
            //trans.Commit();
            connPool.ConnectionClose();
            return true;
        }

        public bool GetEditYoutubeMeta(DataSet ds)
        {
            connPool.ConnectionOpen();
            m_sql = String.Format(@"SELECT
                                    cid
                                    , videoid
                                    , assetid
                                    , playlist_id
                                    , title
                                    , description
                                    , tag
                                    , isuse
                                    , category
                                    , spoken_language
                                    , target_language
                                    , org_lang_title
                                    , org_lang_desc
                                    , trans_lang_title
                                    , trans_lang_desc
                                    , session_id
                                    , tms_id
                                    , isan
                                    , eidr
                                    , season
                                    , episode
                                    , custom_id
                                    FROM TB_YOUTUBE YT                                    
                                    WHERE 1=1
                                    AND YT.status = 'Edit'
                                    LIMIT 0,1");
            //MySqlTransaction trans = connPool.getConnection().BeginTransaction();
            MySqlDataAdapter adpt = new MySqlDataAdapter(m_sql, connPool.getConnection());
            adpt.Fill(ds, "E_YT");
            //trans.Commit();
            connPool.ConnectionClose();
            return true;
        }

        public bool GetCallbakInfo(DataSet ds)
        {
            connPool.ConnectionOpen();
            m_sql = String.Format(@"SELECT
                                    CB.callback_pk
                                    , CB.tc_pk
                                    , CB.program_seq_pk
                                    , CB.clip_pk
                                    , CB.ftppath
                                    , CB.pathfilename
                                    , CB.profileid
                                    , CB.encset
                                    , CB.encid
                                    , CB.status
                                    , CB.drm
                                    , CB.requestid                                    
                                    , CB.jobid
                                    , CB.taskid
                                    , C.gid as gid
                                    , C.cid as cid
                                    , F.pid as pid
                                    , IFNULL(C.metahub_YN, 'N') AS metahub_YN                                    
                                    , IFNULL(C.transcode_YN, 'Y') AS transcode_YN
                                    FROM TB_CALLBACK CB
                                    LEFT JOIN TB_CLIP C ON CB.clip_pk = C.clip_pk
                                    LEFT JOIN TB_FTP_QUEUE F ON F.tc_pk = CB.tc_pk
                                    WHERE 1=1
                                    AND CB.status = 'Pending'
                                    LIMIT 0,1");
            //MySqlTransaction trans = connPool.getConnection().BeginTransaction();
            MySqlDataAdapter adpt = new MySqlDataAdapter(m_sql, connPool.getConnection());
            adpt.Fill(ds, "CDN");
            //trans.Commit();
            connPool.ConnectionClose();
            return true;
        }

        public bool UpdateProgramStatus(String type, String pid)
        {
            connPool.ConnectionOpen();
            m_sql = String.Format("UPDATE TB_PROGRAM SET job_endtime = CURRENT_TIMESTAMP(), status = '{0}' WHERE pid = '{1}'", "Completed", pid);
            //Running 으로 변경
            cmd = new MySqlCommand(m_sql, connPool.getConnection());
            cmd.ExecuteNonQuery();
            connPool.ConnectionClose();
            return true;
        }

        public bool UpdateSmrProgramStatus(String type, String pid)
        {
            connPool.ConnectionOpen();
            m_sql = String.Format("UPDATE TB_SMR_PROGRAM SET edit_time = CURRENT_TIMESTAMP(), status = 'Completed' WHERE pid = '{0}'", pid);
            //Running 으로 변경
            cmd = new MySqlCommand(m_sql, connPool.getConnection());
            cmd.ExecuteNonQuery();
            connPool.ConnectionClose();
            return true;
        }

        public bool InsertCLIPINFO(String gid, String cid, String typeid, String hls_url)
        {
            hls_url = hls_url.Replace("http://vod.", "http://idolvod.");
            hls_url = hls_url.Replace("mbcplusvod/_definst_", "idolvod/_definst_");
            hls_url = hls_url.Replace("mp4:mbcplus_mbcpvod", "mp4:mbcplus_mbcpidol");
            connPool.ConnectionOpen();
            m_sql = String.Format(@"INSERT INTO TB_CLIP_INFO(insert_time, edit_date, gid, cid, typeid, url, pooq_itemid)
                                                  VALUES (CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), '{0}', '{1}', '{2}', '{3}', '0')
                                                  ON DUPLICATE KEY UPDATE edit_date = CURRENT_TIMESTAMP(), gid = '{0}', cid = '{1}', typeid = '{2}', url = '{3}'"
                , gid, cid, typeid, hls_url);

            cmd = new MySqlCommand(m_sql, connPool.getConnection());
            cmd.ExecuteNonQuery();
            connPool.ConnectionClose();
            return true;
        }

        public bool GetArchiveProgramSeqServiceInfo(DataSet ds)
        {
            connPool.ConnectionOpen();
            m_sql = String.Format(@"SELECT
                                    PS.program_seq_pk
                                    , PS.imgsrcpath
                                    , PS.orgimgname
                                    , PS.contentid
                                    , PS.gid
                                    , DATE_FORMAT(PS.insert_time, '%Y\\%m\\%d') AS archive_date
                                    , PS.cdnurl_img
                                    , PS.status
                                    , P.section AS section
                                    , (SELECT COUNT(*) FROM TB_EDIT_INFO E WHERE E.id = PS.gid AND (E.type = 'img' AND E.board_type = 'program_seq')) AS edit_img_count
                                    FROM TB_PROGRAM_SEQ PS
                                    LEFT JOIN TB_PROGRAM P ON PS.pid = P.pid
                                    WHERE PS.STATUS = 'Pending'
                                    LIMIT 0,1");
            //MySqlTransaction trans = connPool.getConnection().BeginTransaction();
            MySqlDataAdapter adpt = new MySqlDataAdapter(m_sql, connPool.getConnection());
            adpt.Fill(ds, "ARCHIVE_PROGRAM");
            //trans.Commit();
            connPool.ConnectionClose();
            return true;
        }

        public bool GetProgramService(DataSet ds)
        {
            connPool.ConnectionOpen();
            m_sql = String.Format(@"SELECT
                                P.seq as pk
                                , P.pid as pid          
                                , P.imgsrcpath as src_img
                                , P.orgimgname as org_img
                                , P.src_poster_img as src_poster_img
                                , P.org_poster_img as org_poster_img
                                , P.src_thumb_img as src_thumb_img
                                , P.org_thumb_img as org_thumb_img
                                , P.src_circle_img as src_circle_img
                                , P.org_circle_img as org_circle_img
                                , P.status as status
, (SELECT COUNT(*) FROM TB_EDIT_INFO E WHERE E.id = P.pid AND (E.type = 'img' AND E.board_type = 'program')) AS edit_img_count
, (SELECT COUNT(*) FROM TB_EDIT_INFO E WHERE E.id = P.pid AND (E.type = 'posterimg' AND E.board_type = 'program')) AS edit_img_poster_count
, (SELECT COUNT(*) FROM TB_EDIT_INFO E WHERE E.id = P.pid AND (E.type = 'thumbimg' AND E.board_type = 'program')) AS edit_img_thumb_count
, (SELECT COUNT(*) FROM TB_EDIT_INFO E WHERE E.id = P.pid AND (E.type = 'circleimg' AND E.board_type = 'program')) AS edit_img_circle_count
                                FROM TB_PROGRAM P
                                WHERE 1=1
                                AND status = 'Pending'
                                LIMIT 0,1");
            //MySqlTransaction trans = connPool.getConnection().BeginTransaction();
            MySqlDataAdapter adpt = new MySqlDataAdapter(m_sql, connPool.getConnection());
            adpt.Fill(ds, "PROGRAM");
            //trans.Commit();
            connPool.ConnectionClose();
            return true;
        }

        public bool GetProgramSmrService(DataSet ds)
        {
            connPool.ConnectionOpen();
            m_sql = String.Format(@"SELECT                                
                                SP.pid as pid          
                                , SP.img as img
                                , SP.org_img as org_img
                                , SP.posterimg1 as posterimg1
                                , SP.org_posterimg1 as org_posterimg1
                                , SP.posterimg2 as posterimg2
                                , SP.org_posterimg2 as org_posterimg2
                                , SP.bannerimg as bannerimg
                                , SP.org_bannerimg as org_bannerimg                                
                                , SP.thumbimg as thumbimg
                                , SP.org_thumbimg as org_thumbimg
                                , SP.status as status
, (SELECT COUNT(*) FROM TB_EDIT_INFO E WHERE E.id = SP.pid AND (E.type = 'img' AND E.board_type = 'smr_program')) AS edit_img_count
, (SELECT COUNT(*) FROM TB_EDIT_INFO E WHERE E.id = SP.pid AND (E.type = 'posterimg1' AND E.board_type = 'smr_program')) AS edit_img_poster1_count
, (SELECT COUNT(*) FROM TB_EDIT_INFO E WHERE E.id = SP.pid AND (E.type = 'posterimg2' AND E.board_type = 'smr_program')) AS edit_img_poster2_count
, (SELECT COUNT(*) FROM TB_EDIT_INFO E WHERE E.id = SP.pid AND (E.type = 'thumbimg' AND E.board_type = 'smr_program')) AS edit_img_thumb_count
, (SELECT COUNT(*) FROM TB_EDIT_INFO E WHERE E.id = SP.pid AND (E.type = 'bannerimg' AND E.board_type = 'smr_program')) AS edit_img_banner_count
                                FROM TB_SMR_PROGRAM SP
                                WHERE 1=1
                                AND status = 'Pending'
                                LIMIT 0,1");
            //MySqlTransaction trans = connPool.getConnection().BeginTransaction();
            MySqlDataAdapter adpt = new MySqlDataAdapter(m_sql, connPool.getConnection());
            adpt.Fill(ds, "SMR_PROGRAM");
            //trans.Commit();
            connPool.ConnectionClose();
            return true;
        }

        public bool ArchiveProgram(vo.ProgramInfo programInfo)
        {
            connPool.ConnectionOpen();
            
            if (!String.IsNullOrEmpty(programInfo.img))
            {
                String edit_count_string = "";
                if (programInfo.edit_img_count > 1)
                {
                    edit_count_string = String.Format("_{0}", (programInfo.edit_img_count - 1).ToString("D2"));
                }
                m_sql = String.Format(@"INSERT INTO TB_ARCHIVE(insert_time, pid, srcpath, targetpath, type, status, program_img_type, edit_count_tail)
                                                  VALUES (CURRENT_TIMESTAMP(), '{0}', '{1}', '{2}', 'IMG', 'Pending', 1, '{3}')", programInfo.pid
                                                  , Util.escapedPath(programInfo.img)
                                                  , Util.escapedPath(programInfo.targetpath + Path.DirectorySeparatorChar + programInfo.pid + edit_count_string + Path.GetExtension(programInfo.org_img.ToLower()))
                                                  , edit_count_string);

                cmd = new MySqlCommand(m_sql, connPool.getConnection());
                cmd.ExecuteNonQuery();
            }

            if (!String.IsNullOrEmpty(programInfo.posterimg))
            {
                String edit_count_string = "";
                if (programInfo.edit_img_poster_count > 1)
                {
                    edit_count_string = String.Format("_{0}", (programInfo.edit_img_poster_count - 1).ToString("D2"));
                }
                m_sql = String.Format(@"INSERT INTO TB_ARCHIVE(insert_time, pid, srcpath, targetpath, type, status, program_img_type, edit_count_tail)
                                                  VALUES (CURRENT_TIMESTAMP(), '{0}', '{1}', '{2}', 'IMG', 'Pending', 2, '{3}')", programInfo.pid
                                                      , Util.escapedPath(programInfo.posterimg)
                                                      , Util.escapedPath(programInfo.targetpath + Path.DirectorySeparatorChar + programInfo.pid + "_P" + edit_count_string + Path.GetExtension(programInfo.org_posterimg.ToLower()))
                                                      , edit_count_string);
                cmd = new MySqlCommand(m_sql, connPool.getConnection());
                cmd.ExecuteNonQuery();
            }

            if (!String.IsNullOrEmpty(programInfo.thumbimg))
            {
                String edit_count_string = "";
                if (programInfo.edit_img_thumb_count > 1)
                {
                    edit_count_string = String.Format("_{0}", (programInfo.edit_img_thumb_count - 1).ToString("D2"));
                }

                m_sql = String.Format(@"INSERT INTO TB_ARCHIVE(insert_time, pid, srcpath, targetpath, type, status, program_img_type, edit_count_tail)
                                                  VALUES (CURRENT_TIMESTAMP(), '{0}', '{1}', '{2}', 'IMG', 'Pending', 3, '{3}')", programInfo.pid
                                                  , Util.escapedPath(programInfo.thumbimg)
                                                  , Util.escapedPath(programInfo.targetpath + Path.DirectorySeparatorChar + programInfo.pid + "_T" + edit_count_string + Path.GetExtension(programInfo.org_thumbimg.ToLower()))
                                                  , edit_count_string);
                cmd = new MySqlCommand(m_sql, connPool.getConnection());
                cmd.ExecuteNonQuery();
            }

            if (!String.IsNullOrEmpty(programInfo.circleimg))
            {
                String edit_count_string = "";
                if (programInfo.edit_img_circle_count > 1)
                {
                    edit_count_string = String.Format("_{0}", (programInfo.edit_img_circle_count - 1).ToString("D2"));
                }

                m_sql = String.Format(@"INSERT INTO TB_ARCHIVE(insert_time, pid, srcpath, targetpath, type, status, program_img_type, edit_count_tail)
                                                  VALUES (CURRENT_TIMESTAMP(), '{0}', '{1}', '{2}', 'IMG', 'Pending', 4, '{3}')", programInfo.pid
                                                  , Util.escapedPath(programInfo.circleimg)
                                                  , Util.escapedPath(programInfo.targetpath + Path.DirectorySeparatorChar + programInfo.pid + "_C" + edit_count_string + Path.GetExtension(programInfo.org_circleimg.ToLower()))
                                                  , edit_count_string);
                cmd = new MySqlCommand(m_sql, connPool.getConnection());
                cmd.ExecuteNonQuery();
            }

            connPool.ConnectionClose();
            return true;
        }

        public bool ArchiveProgramSeq(String pk, String sourcePath, String destPath, String edit_count_string)
        {
            connPool.ConnectionOpen();
            m_sql = String.Format(@"INSERT INTO TB_ARCHIVE(insert_time, program_seq_pk, srcpath, targetpath, type, status, edit_count_tail)
                                                VALUES (CURRENT_TIMESTAMP(), '{0}', '{1}', '{2}', 'IMG', 'Pending', '{3}')", pk
                        , sourcePath
                        , destPath
                        , edit_count_string);

            cmd = new MySqlCommand(m_sql, connPool.getConnection());
            cmd.ExecuteNonQuery();
            connPool.ConnectionClose();
            return true;
        }

        public bool ArchiveClip(String pk, String sourcePath, String destPath, String type, String cid, String edit_count_string)
        {            
            m_sql = String.Format(@"INSERT INTO TB_ARCHIVE(insert_time, clip_pk, srcpath, targetpath, type, status, cid, edit_count_tail)
                                    VALUES (CURRENT_TIMESTAMP(), '{0}', '{1}', '{2}', '{4}', 'Pending', '{3}', '{5}')", pk
                                    , sourcePath
                                    , destPath
                                    , cid
                                    , type
                                    , edit_count_string);

            connPool.ConnectionOpen();
            cmd = new MySqlCommand(m_sql, connPool.getConnection());
            cmd.ExecuteNonQuery();
            connPool.ConnectionClose();
            return true;
        }

        public bool ArchiveSmrProgram(vo.SmrProgramInfo smrProgramInfo)
        {
            connPool.ConnectionOpen();

            if (!String.IsNullOrEmpty(smrProgramInfo.img))
            {
                String edit_count_string = "";
                if (smrProgramInfo.edit_img_count > 1)
                {
                    edit_count_string = String.Format("_{0}", (smrProgramInfo.edit_img_count - 1).ToString("D2"));
                }
                m_sql = String.Format(@"INSERT INTO TB_ARCHIVE(insert_time, smr_pid, srcpath, targetpath, type, status, smr_img_type, edit_count_tail)
                                        VALUES (CURRENT_TIMESTAMP(), '{0}', '{1}', '{2}', 'IMG', 'Pending', 1, '{3}')", smrProgramInfo.pid
                                       , Util.escapedPath(smrProgramInfo.img)
                                       , Util.escapedPath(smrProgramInfo.targetpath + Path.DirectorySeparatorChar + smrProgramInfo.pid + edit_count_string + Path.GetExtension(smrProgramInfo.org_img.ToLower()))
                                       , edit_count_string);

                cmd = new MySqlCommand(m_sql, connPool.getConnection());
                cmd.ExecuteNonQuery();
            }

            if (!String.IsNullOrEmpty(smrProgramInfo.posterimg1))
            {
                String edit_count_string = "";
                if (smrProgramInfo.edit_img_poster1_count > 1)
                {
                    edit_count_string = String.Format("_{0}", (smrProgramInfo.edit_img_poster1_count - 1).ToString("D2"));
                }
                m_sql = String.Format(@"INSERT INTO TB_ARCHIVE(insert_time, smr_pid, srcpath, targetpath, type, status, smr_img_type, edit_count_tail)
                                        VALUES (CURRENT_TIMESTAMP(), '{0}', '{1}', '{2}', 'IMG', 'Pending', 2, '{3}')", smrProgramInfo.pid
                                        , Util.escapedPath(smrProgramInfo.posterimg1)
                                        , Util.escapedPath(smrProgramInfo.targetpath + Path.DirectorySeparatorChar + smrProgramInfo.pid + "_P1" + edit_count_string + Path.GetExtension(smrProgramInfo.org_posterimg1.ToLower()))
                                        , edit_count_string);
                cmd = new MySqlCommand(m_sql, connPool.getConnection());
                cmd.ExecuteNonQuery();
            }

            if (!String.IsNullOrEmpty(smrProgramInfo.posterimg2))
            {
                String edit_count_string = "";
                if (smrProgramInfo.edit_img_poster2_count > 1)
                {
                    edit_count_string = String.Format("_{0}", (smrProgramInfo.edit_img_poster2_count - 1).ToString("D2"));
                }
                m_sql = String.Format(@"INSERT INTO TB_ARCHIVE(insert_time, smr_pid, srcpath, targetpath, type, status, smr_img_type, edit_count_tail)
                                                  VALUES (CURRENT_TIMESTAMP(), '{0}', '{1}', '{2}', 'IMG', 'Pending', 3, '{3}')", smrProgramInfo.pid
                                                  , Util.escapedPath(smrProgramInfo.posterimg2)
                                                  , Util.escapedPath(smrProgramInfo.targetpath + Path.DirectorySeparatorChar + smrProgramInfo.pid + "_P2" + edit_count_string + Path.GetExtension(smrProgramInfo.org_posterimg2.ToLower()))
                                                  , edit_count_string);
                cmd = new MySqlCommand(m_sql, connPool.getConnection());
                cmd.ExecuteNonQuery();
            }

            if (!String.IsNullOrEmpty(smrProgramInfo.bannerimg))
            {
                String edit_count_string = "";
                if (smrProgramInfo.edit_img_banner_count > 1)
                {
                    edit_count_string = String.Format("_{0}", (smrProgramInfo.edit_img_banner_count - 1).ToString("D2"));
                }
                m_sql = String.Format(@"INSERT INTO TB_ARCHIVE(insert_time, smr_pid, srcpath, targetpath, type, status, smr_img_type, edit_count_tail)
                                                  VALUES (CURRENT_TIMESTAMP(), '{0}', '{1}', '{2}', 'IMG', 'Pending', 4, '{3}')", smrProgramInfo.pid
                                                  , Util.escapedPath(smrProgramInfo.bannerimg)
                                                  , Util.escapedPath(smrProgramInfo.targetpath + Path.DirectorySeparatorChar + smrProgramInfo.pid + "_B" + edit_count_string + Path.GetExtension(smrProgramInfo.org_bannerimg.ToLower()))
                                                  , edit_count_string);
                cmd = new MySqlCommand(m_sql, connPool.getConnection());
                cmd.ExecuteNonQuery();
            }

            if (!String.IsNullOrEmpty(smrProgramInfo.thumbimg))
            {
                String edit_count_string = "";
                if (smrProgramInfo.edit_img_thumb_count > 1)
                {
                    edit_count_string = String.Format("_{0}", (smrProgramInfo.edit_img_thumb_count - 1).ToString("D2"));
                }
                m_sql = String.Format(@"INSERT INTO TB_ARCHIVE(insert_time, smr_pid, srcpath, targetpath, type, status, smr_img_type, edit_count_tail)
                                                  VALUES (CURRENT_TIMESTAMP(), '{0}', '{1}', '{2}', 'IMG', 'Pending', 5, '{3}')", smrProgramInfo.pid
                                                  , Util.escapedPath(smrProgramInfo.thumbimg)
                                                  , Util.escapedPath(smrProgramInfo.targetpath + Path.DirectorySeparatorChar + smrProgramInfo.pid + "_T" + edit_count_string + Path.GetExtension(smrProgramInfo.org_thumbimg.ToLower()))
                                                  , edit_count_string);
                cmd = new MySqlCommand(m_sql, connPool.getConnection());
                cmd.ExecuteNonQuery();
            }

            connPool.ConnectionClose();
            return true;
        }

        public bool GetArchiveClipService(DataSet ds)
        {            
            m_sql = String.Format(@"SELECT
                                    C.clip_pk
                                    , C.imgsrcpath
                                    , C.clipsrcpath
                                    , C.subtitlesrcpath
                                    , YT.upload_img as yt_upload_img
                                    , YT.upload_srt as yt_upload_srt
                                    , C.orgimgname
                                    , C.orgclipname
                                    , C.orgsubtitlename
                                    , YT.org_img as yt_org_img
                                    , YT.org_srt as yt_org_srt
                                    , C.gid
                                    , C.cid
                                    , C.cdnurl_img
                                    , C.cdnurl_mov
                                    , C.cdnurl_subtitle                                    
                                    , YT.cdn_img as yt_cdn_img
                                    , YT.cdn_srt as yt_cdn_srt
                                    , IFNULL(C.metahub_YN, 'Y') AS metahub_YN
                                    , (SELECT DATE_FORMAT(C.insert_time, '%Y\\%m\\%d') FROM TB_PROGRAM_SEQ PS WHERE PS.gid = C.gid ) AS archive_date
                                    , P.section AS section
                                    , C.status
                                    , C.idolvod_YN as idolvod_YN
                                    , C.idolclip_YN as idolclip_YN
                                    , C.idolvote_YN as idolvote_YN
                                    , C.isvod as isvod
                                    , YT.isuse AS yt_isuse
                                    , YT.videoid as yt_videoid
                                    , DM.isuse AS dm_isuse
                                    , DM.videoid as dm_videoid                                    
                                    , C.archive_img AS archive_img
                                    , C.archive_clip AS archive_clip
                                    , C.archive_subtitle as archive_subtitle                                    
                                    , YT.archive_img as yt_ar_img
                                    , YT.archive_srt as yt_ar_srt
                                    , YT.type as yt_type
                                    , (SELECT COUNT(*) FROM TB_EDIT_INFO E WHERE E.id = C.cid AND (E.type = 'img' AND E.board_type = 'clip')) AS edit_img_count
                                    , (SELECT COUNT(*) FROM TB_EDIT_INFO E WHERE E.id = C.cid AND (E.type = 'clip' AND E.board_type = 'clip')) AS edit_clip_count
                                    , (SELECT COUNT(*) FROM TB_EDIT_INFO E WHERE E.id = C.cid AND (E.type = 'img' AND E.board_type = 'vod')) AS edit_vod_img_count
                                    , (SELECT COUNT(*) FROM TB_EDIT_INFO E WHERE E.id = C.cid AND (E.type = 'vod' AND E.board_type = 'vod')) AS edit_vod_clip_count
                                    , C.isuse as isuse
                                    FROM TB_CLIP C
                                    LEFT JOIN TB_PROGRAM_SEQ PS ON PS.gid = C.gid
                                    LEFT JOIN TB_PROGRAM P ON P.pid = PS.pid                                            
                                    LEFT JOIN TB_YOUTUBE YT ON YT.cid = C.cid AND YT.type = 'SMR'
                                    LEFT JOIN TB_DAILYMOTION DM ON DM.cid = C.cid
                                    WHERE C.STATUS = 'Pending'
                                    ");
            connPool.ConnectionOpen();
            //MySqlTransaction trans = connPool.getConnection().BeginTransaction();
            MySqlDataAdapter adpt = new MySqlDataAdapter(m_sql, connPool.getConnection());
            adpt.Fill(ds, "ARCHIVE");
            //trans.Commit();
            connPool.ConnectionClose();
            return true;
        }

        public DataSet GetYoutubeITEMS(String cid)
        {
            DataSet ds = new DataSet();
            //vo.YtItems ytItems = new vo.YtItems();
            connPool.ConnectionOpen();
            m_sql = String.Format(@"SELECT YI.type as type, YI.org_name as org_name, YI.upload_path as upload_path, YI.language as language FROM TB_YT_ITEMS YI WHERE cid = '{0}' ", cid);
                
            MySqlDataAdapter adpt = new MySqlDataAdapter(m_sql, connPool.getConnection());
            adpt.Fill(ds, "GET_YT_ITEMS");
            connPool.ConnectionClose();

            return ds;
        }

        public bool GetIdolChampCheck(String cid)
        {
            DataSet ds = new DataSet();
            connPool.ConnectionOpen();
            m_sql = String.Format(@"SELECT
                                     cid, typeid, pooq_itemid                                     
                                     FROM TB_CLIP_INFO
                                     WHERE 1=1
                                     AND cid = '{0}'
                                     AND (typeid = 2 OR typeid = 3)
                                     AND pooq_itemid = 0
                                     AND url is NOT NULL", cid);
            MySqlDataAdapter adpt = new MySqlDataAdapter(m_sql, connPool.getConnection());
            adpt.Fill(ds, "CHECK_IDOL");
            connPool.ConnectionClose();
            logger.logging("idol check : " + ds.Tables[0].Rows.Count.ToString());
            if (ds.Tables[0].Rows.Count >= 2)
            {                
                ds.Clear();
                return true;                
            }
            return false;
        }

        public bool SetAdditionalTranscoding(String pk, String gid, String cid, out String srcPath)
        {
            DataSet ds = new DataSet();
            connPool.ConnectionOpen();
            m_sql = String.Format(@"SELECT targetpath FROM TB_ARCHIVE WHERE cid = '{0}'
                                    AND STATUS = 'Completed'
                                    AND TYPE = 'MOV'
                                    LIMIT 0,1", cid);
            MySqlDataAdapter adpt = new MySqlDataAdapter(m_sql, connPool.getConnection());
            adpt.Fill(ds, "CHECK_URL");
            connPool.ConnectionClose();

            logger.logging("target check : " + ds.Tables[0].Rows.Count.ToString());
            if (ds.Tables[0].Rows.Count > 0)
            {
                logger.logging(ds.Tables[0].Rows[0]["targetpath"].ToString());
                String ftptargetpath = "";
                srcPath = ds.Tables[0].Rows[0]["targetpath"].ToString();
                ftptargetpath = srcPath;
                ftptargetpath = System.IO.Path.GetDirectoryName(ftptargetpath);
                ftptargetpath = ftptargetpath.Replace(@"Z:\", "");
                ftptargetpath = ftptargetpath.Replace(@"\", "/");                
                connPool.ConnectionOpen();
                m_sql = String.Format(@"INSERT INTO TB_FTP_QUEUE (starttime, clip_pk, srcpath, targetfilename, status, type, customer_id, targetpath, gid, cid)
                                                VALUES( CURRENT_TIMESTAMP(), '{0}', '{1}', '{2}', 'Pending', '{3}', '{4}', '{5}', '{6}', '{7}')", pk
                                                                                                                        , Util.escapedPath(srcPath)
                                                                                                                        , System.IO.Path.GetFileName(srcPath)
                                                                                                                        , "MOV"                                                                                                                        
                                                                                                                        , "1" // BBMC
                                                                                                                        , ftptargetpath
                                                                                                                        , gid
                                                                                                                        , cid);
                cmd = new MySqlCommand(m_sql, connPool.getConnection());
                cmd.ExecuteNonQuery();
                connPool.ConnectionClose();                
                
                return true;
            }
            srcPath = null;
            return false;
        }

        public bool SetAdditionalCustomer(ClipInfo clipInfo, String customer_id, String type)
        {
            String srcPath = null;
            String ftptargetpath = null;
            if ( type == "IMG")
            {
                srcPath = clipInfo.archive_img;                                
            }
            else if ( type == "MOV")
            {
                srcPath = clipInfo.archive_clip;
            }
            ftptargetpath = srcPath;
            srcPath = String.Format(@"Z:\{0}", srcPath);
            srcPath = srcPath.Replace("/", @"\");
            
            connPool.ConnectionOpen();
            m_sql = String.Format(@"INSERT INTO TB_FTP_QUEUE (starttime, clip_pk, srcpath, targetfilename, status, type, customer_id, targetpath, gid, cid)
                                            VALUES( CURRENT_TIMESTAMP(), '{0}', '{1}', '{2}', 'Pending', '{3}', '{4}', '{5}', '{6}', '{7}')", clipInfo.clip_pk
                                                                                                                    , Util.escapedPath(srcPath)
                                                                                                                    , System.IO.Path.GetFileName(srcPath)
                                                                                                                    , type
                                                                                                                    , customer_id
                                                                                                                    , ftptargetpath
                                                                                                                    , clipInfo.gid
                                                                                                                    , clipInfo.cid);
            //logger.logging(m_sql);
            cmd = new MySqlCommand(m_sql, connPool.getConnection());
            cmd.ExecuteNonQuery();
            connPool.ConnectionClose();
            return true;
        }

        public bool PutArchiveToFtp(vo.YoutubeContentInfo youtubeContentInfo)
        {
            UpdateYoutubeStatus(youtubeContentInfo.cid, "Running");
            String srcPath = null, targetPath = null, targetfileName;
            
            //logger.logging(String.Format("srcimg : {0}, srcmovie : {1}", youtubeContentInfo.srcImg, youtubeContentInfo.srcMovie));

            if ( !String.IsNullOrEmpty(youtubeContentInfo.srcImg) )
            {
                srcPath = youtubeContentInfo.srcImg;
                srcPath = String.Format("{0}{1}", @"Z:", srcPath).Replace("/", @"\");

                targetPath = Path.GetDirectoryName(youtubeContentInfo.srcImg).Replace(@"\", "/");
                targetfileName = Path.GetFileName(youtubeContentInfo.srcImg);

                logger.logging(String.Format("srcpath : {0}, targetPath : {1}, targetfileName : {2}", srcPath, targetPath, targetfileName));

                connPool.ConnectionOpen();
                m_sql = String.Format(@"INSERT INTO TB_FTP_QUEUE (starttime, srcpath, targetfilename, status, type, customer_id, targetpath, cid, gid, clip_pk)
                                        VALUES( CURRENT_TIMESTAMP(), {0}, '{1}', 'Pending', '{2}', '{3}', '{4}', '{5}', '{6}', '{7}')"
                                        /*0 srcpath*/, "@srcPath"
                                        /*1 targetfilename*/, targetfileName
                                        /*2 type */, "IMG"
                                        /*3 customer_id*/, youtubeContentInfo.customerId
                                        /*4 targetpath*/, targetPath
                                        /*5 cid*/, youtubeContentInfo.cid
                                        /*6 gid*/, youtubeContentInfo.gid
                                        /*7 clip_pk*/, youtubeContentInfo.clip_pk);
                cmd = new MySqlCommand(m_sql, connPool.getConnection());
                cmd.Parameters.Add(new MySqlParameter("@srcPath", srcPath));
                cmd.ExecuteNonQuery();
                connPool.ConnectionClose();
            } else
            {
                // 유튜브 업로드용 이미지 없으면 실패                
                UpdateYoutubeStatus(youtubeContentInfo.cid, "Failed");                
                return false;
            }
            // 자막 있을 경우 자막 처리 해야함(2019-04-19 미구현)

            if (!String.IsNullOrEmpty(youtubeContentInfo.srcMovie))
            {
                srcPath = youtubeContentInfo.srcMovie;
                srcPath = String.Format("{0}{1}", @"Z:", srcPath).Replace("/", @"\");

                targetPath = Path.GetDirectoryName(youtubeContentInfo.srcMovie).Replace(@"\", "/");
                targetfileName = Path.GetFileName(youtubeContentInfo.srcMovie);

                logger.logging(String.Format("srcpath : {0}, targetPath : {1}, targetfileName : {2}", srcPath, targetPath, targetfileName));

                connPool.ConnectionOpen();
                m_sql = String.Format(@"INSERT INTO TB_FTP_QUEUE (starttime, srcpath, targetfilename, status, type, customer_id, targetpath, cid, gid, clip_pk)
                                        VALUES( CURRENT_TIMESTAMP(), {0}, '{1}', 'Pending', '{2}', '{3}', '{4}', '{5}', '{6}', '{7}')"
                                        /*0 srcpath*/, "@srcPath"
                                        /*1 targetfilename*/, targetfileName
                                        /*2 type */, "MOV"
                                        /*3 customer_id*/, youtubeContentInfo.customerId
                                        /*4 targetpath*/, targetPath
                                        /*5 cid*/, youtubeContentInfo.cid
                                        /*6 gid*/, youtubeContentInfo.gid
                                        /*7 clip_pk*/, youtubeContentInfo.clip_pk);
                cmd = new MySqlCommand(m_sql, connPool.getConnection());
                cmd.Parameters.Add(new MySqlParameter("@srcPath", srcPath));
                cmd.ExecuteNonQuery();
                connPool.ConnectionClose();
            }
            return true;
        }

        public bool PutArchiveToFtp(vo.DailymotionContentInfo dailymotionContentInfo)
        {
            UpdateDailyMotionStatus(dailymotionContentInfo.cid, "Running");
            String srcPath = null, targetPath = null, targetfileName;

            //logger.logging(String.Format("srcimg : {0}, srcmovie : {1}", dailymotionContentInfo.srcImg, dailymotionContentInfo.srcMovie));

            if (!String.IsNullOrEmpty(dailymotionContentInfo.srcImg))
            {
                srcPath = dailymotionContentInfo.srcImg;
                srcPath = String.Format("{0}{1}", @"Z:", srcPath).Replace("/", @"\");

                targetPath = Path.GetDirectoryName(dailymotionContentInfo.srcImg).Replace(@"\", "/");
                targetfileName = Path.GetFileName(dailymotionContentInfo.srcImg);

                logger.logging(String.Format("srcpath : {0}, targetPath : {1}, targetfileName : {2}", srcPath, targetPath, targetfileName));

                connPool.ConnectionOpen();
                m_sql = String.Format(@"INSERT INTO TB_FTP_QUEUE (starttime, srcpath, targetfilename, status, type, customer_id, targetpath, cid)
                                        VALUES( CURRENT_TIMESTAMP(), {0}, '{1}', 'Pending', '{2}', '{3}', '{4}', '{5}')"
                                        /*0 srcpath*/, "@srcPath"
                                        /*1 targetfilename*/, targetfileName
                                        /*2 type */, "IMG"
                                        /*3 customer_id*/, dailymotionContentInfo.customerId
                                        /*4 targetpath*/, targetPath
                                        /*5*/, dailymotionContentInfo.cid);
                cmd = new MySqlCommand(m_sql, connPool.getConnection());
                cmd.Parameters.Add(new MySqlParameter("@srcPath", srcPath));
                cmd.ExecuteNonQuery();
                connPool.ConnectionClose();
            }
            else
            {
                UpdateDailyMotionStatus(dailymotionContentInfo.cid, "Failed");
                // 데일리모션 업로드용 이미지 없으면 실패
                return false;
            }
            // 자막 있을 경우 자막 처리 해야함(2019-04-19 미구현)
            if (!String.IsNullOrEmpty(dailymotionContentInfo.srcSubtitle))
            {
                
            }

            if (!String.IsNullOrEmpty(dailymotionContentInfo.srcMovie))
            {
                srcPath = dailymotionContentInfo.srcMovie;
                srcPath = String.Format("{0}{1}", @"Z:", srcPath).Replace("/", @"\");

                targetPath = Path.GetDirectoryName(dailymotionContentInfo.srcMovie).Replace(@"\", "/");
                targetfileName = Path.GetFileName(dailymotionContentInfo.srcMovie);

                logger.logging(String.Format("srcpath : {0}, targetPath : {1}, targetfileName : {2}", srcPath, targetPath, targetfileName));

                connPool.ConnectionOpen();
                m_sql = String.Format(@"INSERT INTO TB_FTP_QUEUE (starttime, srcpath, targetfilename, status, type, customer_id, targetpath, cid)
                                        VALUES( CURRENT_TIMESTAMP(), {0}, '{1}', 'Pending', '{2}', '{3}', '{4}', '{5}')"
                                        /*0 srcpath*/, "@srcPath"
                                        /*1 targetfilename*/, targetfileName
                                        /*2 type */, "MOV"
                                        /*3 customer_id*/, dailymotionContentInfo.customerId
                                        /*4 targetpath*/, targetPath
                                        /*5*/, dailymotionContentInfo.cid);
                cmd = new MySqlCommand(m_sql, connPool.getConnection());
                cmd.Parameters.Add(new MySqlParameter("@srcPath", srcPath));
                cmd.ExecuteNonQuery();
                connPool.ConnectionClose();
            }
            return true;
        }

        public bool GetFtpInfo(DataSet ds, String tail)
        {
            connPool.ConnectionOpen();            
            m_sql = String.Format(@"SELECT ftp_queue_pk as pk
	                                , IFNULL(FQ.srcpath, '') as srcpath
	                                , IFNULL(FQ.targetpath, '') as targetpath
	                                , IFNULL(FQ.targetfilename, '') as targetfilename
                                    , IFNULL(SUBSTR(targetfilename, 1,2), '') AS attribute
	                                , IFNULL(FQ.clip_pk, '') as clip_pk
                                    , P.pid AS pid
                                    , FQ.pid AS fq_pid
	                                , FQ.gid AS gid
                                    , FQ.cid AS cid
                                    , FQ.smr_pid as smr_pid
	                                , FQ.status AS STATUS
	                                , FQ.type AS TYPE    
                                    , FQ.program_img_type as program_img_type
                                    , FQ.smr_img_type as smr_img_type                                    
	                                , CU.customer_pk AS customer_pk
                                    , CU.customer_id AS customer_id
	                                , CU.customer_name AS customer_name
	                                , CU.ftp_host AS HOST
	                                , CU.ftp_port AS PORT
	                                , CU.path AS path
	                                , CU.ftp_id AS id
	                                , CU.ftp_pw AS pw
	                                , CU.transcoding_YN AS transcoding_YN
	                                , CU.clip_YN AS clip_YN
                                    , CU.alias_YN as alias_YN
	                                , IFNULL(PS.contentid, '') AS s_contentid
	                                , IFNULL(PS.cornerid, '') AS s_cornerid                            
	                                , DATE_FORMAT(PS.phun_onair_ymd, '%Y%m%d') AS s_phun_onair_ymd
                                    , DATE_FORMAT(PS.broaddate, '%Y%m%d') AS s_broaddate
	                                , IFNULL(PS.contentnumber, '') AS s_contentnumber
	                                , IFNULL(PS.cornernumber, '') AS s_cornernumber
	                                , IFNULL(PS.preview, '') AS s_preview
	                                , IFNULL(PS.title, '') AS s_title
	                                , IFNULL(PS.searchkeyword, '') AS s_searchkeyword
	                                , IFNULL(PS.actor, '') AS s_actor
	                                , IFNULL(PS.targetage, '') AS s_targetage
	                                , IFNULL(PS.genre, '') AS s_genre
                                    , PS.metahub_YN as s_metahub_YN
	                                , DATE_FORMAT(PS.insert_time, '%Y\\%m\\%d') AS archive_date	
	                                , DATE_FORMAT(C.broaddate, '%Y%m%d') AS broaddate
	                                , DATE_FORMAT(C.edit_date, '%Y%m%d') AS edit_date
                                    , IFNULL(C.title, '') as title
                                    , IFNULL(C.synopsis, '') as synopsis
	                                , IFNULL(C.searchkeyword, '') as searchkeyword
	                                , IFNULL(C.mediadomain, '') as mediadomain
	                                , IFNULL(C.filepath, '') as filepath
	                                , IFNULL(C.itemtypeid, '') as itemtypeid
	                                , IFNULL(C.cliptype, '') as cliptype
	                                , IFNULL(C.clipcategory, '') as clipcategory
	                                , IFNULL(C.clipcategory_name, '') as clipcategory_name
	                                , IFNULL(C.subcategory, '') as subcategory
	                                , IFNULL(C.contentimg, '') as contentimg
	                                , IFNULL(C.playtime, '') as playtime
                                    , C.transcode_YN as transcode_YN
	                                , '0' AS starttime
	                                , '0' AS endtime
	                                , IFNULL(C.returnlink, '') as returnlink
	                                , IFNULL(C.targetage, '') as targetage
	                                , IFNULL(C.targetnation, '') as targetnation
	                                , IFNULL(C.targetplatform, '') as targetplatform
	                                , C.isuse
	                                , C.userid
	                                , C.channelid
	                                , IFNULL(C.hashtag, '') as hashtag
	                                , DATE_FORMAT(C.filemodifydate, '%Y%m%d%H%i%s') AS filemodifydate
	                                , C.reservedate AS reservedate
	                                , C.linktitle1
	                                , C.linkurl1
	                                , C.linktitle2
	                                , C.linkurl2
	                                , C.linktitle3
	                                , C.linkurl3
	                                , C.linktitle4
	                                , C.linkurl4
	                                , C.linktitle5
	                                , C.linkurl5
	                                , C.isfullvod
	                                , C.targetplatformvalue
	                                , DATE_FORMAT(C.broaddate, '%Y%m%d') AS broaddate
	                                , IFNULL(C.sportscomment, '') as sportscomment
	                                , C.platformisuse
	                                , C.masterclipyn
	                                , IFNULL(C.cdnurl_img, '') as cdnurl_img
	                                , IFNULL(C.cdnurl_mov, '') as cdnurl_mov
	                                , IFNULL(C.cdnurl_mov_T1, '') as cdnurl_mov_T1
	                                , IFNULL(C.cdnurl_mov_T2, '') as cdnurl_mov_T2
	                                , IFNULL(C.v_codec, '') as v_codec
	                                , IFNULL(C.v_bitrate, '') as v_bitrate
	                                , IFNULL(C.v_format, '') as v_format
	                                , IFNULL(C.v_resol_x, '') as v_resol_x
	                                , IFNULL(C.v_resol_y, '') as v_resol_y
	                                , IFNULL(C.v_profile, '') as v_profile
	                                , IFNULL(C.v_cabac, '') as v_cabac
	                                , IFNULL(C.v_gop, '') as v_gop
	                                , IFNULL(C.v_version, '') as v_version
	                                , IFNULL(C.a_codec, '') as a_codec
	                                , IFNULL(C.a_bitrate, '') as a_bitrate
	                                , C.status
	                                , C.ftp_target
                                    , C.metahub_YN as metahub_YN
	                                , IFNULL(C.rtmp_url, '') AS rtmp_url
	                                , IFNULL(C.rtmp_url_T1, '') AS rtmp_url_T1
	                                , IFNULL(C.rtmp_url_T2, '') AS rtmp_url_T2
	                                , IFNULL(C.hls_url, '') AS hls_url
	                                , IFNULL(C.hls_url_T1, '') AS hls_url_T1
	                                , IFNULL(C.hls_url_T2, '') AS hls_url_T2
                                    , DATE_FORMAT(AM.match_date, '%Y%m%d') As match_date
                                    , IFNULL(AM.team1, '') as team1
                                    , IFNULL(AM.team2, '') as team2
                                    , IFNULL(AM.player, '') As player
                                    , IFNULL(AM.inning, '') As inning
                                    , IFNULL(AM.videotype, '') as videotype
                                    , IFNULL(AM.sportskind, '') as sportskind
                                    , IFNULL(AM.divisioncode, '') as divisioncode
                                    , IFNULL(AM.gameid, '') as gameid
                                    , AM.mspl_isuse
                                    , AM.mlb_isuse
                                    , IFNULL(AM.sports_sub, '') as sports_sub
                                    , YT.videoid AS yt_videoid
                                    , YT.channel_id AS yt_channel_id
                                    , YT.playlist_id AS yt_playlist_id
                                    , YT.title AS yt_title
                                    , YT.description AS yt_description
                                    , YT.tag AS yt_tag
                                    , YT.isuse AS yt_isuse
                                    , YT.enable_contentid AS yt_enable_contentid
                                    , YT.uploader_name AS yt_uploader_name
                                    , YT.upload_control_id AS yt_upload_control_id
                                    , YT.usage_policy as yt_usage_policy
                                    , YT.match_policy as yt_match_policy
                                    , YT.category as yt_category
                                    , YT.spoken_language as yt_spoken_language
                                    , YT.target_language as yt_target_language
                                    , YT.custom_id as yt_custom_id
                                    , YT.information AS yt_information
                                    , YT.status as yt_status
                                    , YT.ownership as yt_ownership
                                    , DATE_FORMAT(C.broaddate, '%Y-%m-%d') AS ep_original_release_date
                                    , DATE_FORMAT(YT.start_time, '%Y-%m-%dT%H:%i:%s+09:00') as yt_start_time
                                    , DM.videoid AS dm_videoid
                                    , DM.playlistid AS dm_playlistid
                                    , DM.title AS dm_title
                                    , DM.description AS dm_description
                                    , DM.category as dm_category
                                    , DM.url AS dm_url
                                    , DM.tag AS dm_tag
                                    , DM.geoblock_code
                                    , DM.geoblock_value
                                    , UNIX_TIMESTAMP(DM.publish_date) as dm_publish_date
                                    , UNIX_TIMESTAMP(DM.expiry_date) as dm_expiry_date
                                    , DM.isuse AS dm_isuse
                                    , DM.status as dm_status
                                    , (SELECT COUNT(*) FROM TB_EDIT_INFO E WHERE E.id = C.cid AND (E.type = 'img' AND E.board_type = 'clip')) AS clip_img_edit_count
                                    , (SELECT COUNT(*) FROM TB_EDIT_INFO E WHERE E.id = C.cid AND (E.type = 'clip' AND E.board_type = 'clip')) AS clip_mov_edit_count
                                    , (SELECT COUNT(*) FROM TB_EDIT_INFO E WHERE E.id = P.pid AND (E.type = 'img' AND E.board_type = 'program')) AS program_img_edit_count
, (SELECT COUNT(*) FROM TB_EDIT_INFO E WHERE E.id = P.pid AND (E.type = 'posterimg' AND E.board_type = 'program')) AS program_posterimg_edit_count
, (SELECT COUNT(*) FROM TB_EDIT_INFO E WHERE E.id = P.pid AND (E.type = 'thumbimg' AND E.board_type = 'program')) AS program_thumbimg_edit_count
, (SELECT COUNT(*) FROM TB_EDIT_INFO E WHERE E.id = P.pid AND (E.type = 'circleimg' AND E.board_type = 'program')) AS program_circleimg_edit_count
                                    , (SELECT COUNT(*) FROM TB_EDIT_INFO E WHERE E.id = SP.pid AND (E.type = 'img' AND E.board_type = 'smr_program')) AS smr_program_img_edit_count
, (SELECT COUNT(*) FROM TB_EDIT_INFO E WHERE E.id = SP.pid AND (E.type = 'posterimg1' AND E.board_type = 'smr_program')) AS smr_program_posterimg1_edit_count
, (SELECT COUNT(*) FROM TB_EDIT_INFO E WHERE E.id = SP.pid AND (E.type = 'posterimg2' AND E.board_type = 'smr_program')) AS smr_program_posterimg2_edit_count
, (SELECT COUNT(*) FROM TB_EDIT_INFO E WHERE E.id = SP.pid AND (E.type = 'bannerimg' AND E.board_type = 'smr_program')) AS smr_program_bannerimg_edit_count
, (SELECT COUNT(*) FROM TB_EDIT_INFO E WHERE E.id = SP.pid AND (E.type = 'thumbimg' AND E.board_type = 'smr_program')) AS smr_program_thumbimg_edit_count
                                    , (SELECT COUNT(*) FROM TB_EDIT_INFO E WHERE E.id = PS.gid AND (E.type = 'img' AND E.board_type = 'program_seq')) AS program_seq_img_edit_count
                                    , (SELECT COUNT(*) FROM TB_EDIT_INFO E WHERE E.id = C.cid AND (E.type = 'img' AND E.board_type = 'youtube')) AS youtube_img_edit_count
                                    , (SELECT COUNT(*) FROM TB_EDIT_INFO E WHERE E.id = C.cid AND (E.type = 'img' AND E.board_type = 'dailymotion')) AS dailymotion_img_edit_count
                                    FROM TB_FTP_QUEUE FQ
                                    LEFT JOIN TB_CUSTOMER CU ON FQ.customer_id = CU.customer_id
                                    LEFT JOIN TB_PROGRAM_SEQ PS ON FQ.gid = PS.gid
                                    LEFT JOIN TB_CLIP C ON FQ.clip_pk = C.clip_pk
                                    LEFT JOIN TB_ADD_META AM ON FQ.cid = AM.cid
                                    LEFT JOIN TB_YOUTUBE YT ON FQ.cid = YT.cid
                                    LEFT JOIN TB_DAILYMOTION DM ON FQ.cid = DM.cid
                                    LEFT JOIN TB_PROGRAM P ON P.pid = FQ.pid
                                    LEFT JOIN TB_SMR_PROGRAM SP ON SP.pid = FQ.smr_pid
                                    WHERE 1=1
                                    AND FQ.status = 'Pending'
                                    {0}
                                    ORDER BY ftp_queue_pk ASC
                                    LIMIT 0,1
                                    ", tail);
            //logger.logging(m_sql);
            //MySqlTransaction trans = connPool.getConnection().BeginTransaction();
            MySqlDataAdapter adpt = new MySqlDataAdapter(m_sql, connPool.getConnection());
            adpt.Fill(ds, "FTP");
            //trans.Commit();
            connPool.ConnectionClose();

            if (ds.Tables[0].Rows.Count > 0)
            {
                String pk = ds.Tables[0].Rows[0]["pk"].ToString();
                m_sql = String.Format(@"UPDATE TB_FTP_QUEUE SET starttime = CURRENT_TIMESTAMP(), status = 'Running' WHERE ftp_queue_pk = '{0}'", pk);
                connPool.ConnectionOpen();

                //Running 으로 변경
                cmd = new MySqlCommand(m_sql, connPool.getConnection());
                cmd.ExecuteNonQuery();
                connPool.ConnectionClose();
                logger.logging(String.Format(@"[FTPService] ftp_pk({0}) is Running", pk));
            }

            return true;
        }

        public bool GetCopyPrgramSeqInfo(DataSet ds)
        {
            connPool.ConnectionOpen();
            m_sql = String.Format(@"SELECT
                                     A.archive_pk
                                     , IFNULL(A.edit_count_tail, '') as edit_count_tail
                                     , PS.program_seq_pk AS program_seq_pk
                                     , PS.gid AS gid
                                     , A.srcpath AS srcpath
                                     , A.targetpath AS dstpath
                                     , A.status
                                     , A.type FROM TB_ARCHIVE A
                                     INNER JOIN TB_PROGRAM_SEQ PS ON PS.program_seq_pk = A.program_seq_pk
                                     WHERE 1=1
                                     AND A.status = 'Pending'
                                    LIMIT 0,1");
            //MySqlTransaction trans = connPool.getConnection().BeginTransaction();
            MySqlDataAdapter adpt = new MySqlDataAdapter(m_sql, connPool.getConnection());
            adpt.Fill(ds, "COPY_PROGRAM_SEQ");
            //trans.Commit();
            connPool.ConnectionClose();
            return true;
        }

        public bool GetCopyProgramService(DataSet ds)
        {
            connPool.ConnectionOpen();
            m_sql = String.Format(@"
                SELECT
                 archive_pk as pk
                 , IFNULL(A.edit_count_tail, '') as edit_count_tail
                 , P.pid AS pid
                 , A.srcpath AS srcpath
                 , A.targetpath AS dstpath
                 , A.status AS STATUS
                 , A.type AS TYPE
                 , A.program_img_type as program_img_type
                 FROM TB_ARCHIVE A
                 INNER JOIN TB_PROGRAM P ON P.pid = A.pid
                 WHERE 1=1
                 AND A.status = 'Pending'
                LIMIT 0,1");
            //MySqlTransaction trans = connPool.getConnection().BeginTransaction();
            MySqlDataAdapter adpt = new MySqlDataAdapter(m_sql, connPool.getConnection());
            adpt.Fill(ds, "COPY_PROGRAM");
            //trans.Commit();
            connPool.ConnectionClose();
            return true;
        }

        public bool GetCopySmrProgramService(DataSet ds)
        {
            connPool.ConnectionOpen();
            m_sql = String.Format(@"
                SELECT
                 archive_pk as pk
                 , IFNULL(A.edit_count_tail, '') as edit_count_tail
                 , P.pid AS pid
                 , A.srcpath AS srcpath
                 , A.targetpath AS dstpath
                 , A.status AS STATUS
                 , A.type AS TYPE
                 , A.smr_img_type
                 FROM TB_ARCHIVE A
                 INNER JOIN TB_SMR_PROGRAM P ON P.pid = A.smr_pid
                 WHERE 1=1
                 AND A.status = 'Pending'
                LIMIT 0,1");
            //MySqlTransaction trans = connPool.getConnection().BeginTransaction();
            MySqlDataAdapter adpt = new MySqlDataAdapter(m_sql, connPool.getConnection());
            adpt.Fill(ds, "COPY_SMR_PROGRAM");
            //trans.Commit();
            connPool.ConnectionClose();
            return true;
        }

        public bool GetClipService(DataSet ds)
        {
            m_sql = String.Format(@"SELECT
                                PS.contentid AS s_contentid
                                , PS.cornerid AS s_cornerid                            
                                , DATE_FORMAT(PS.phun_onair_ymd, '%Y%m%d') AS s_phun_onair_ymd
                                , PS.contentnumber AS s_contentnumber
                                , PS.cornernumber AS s_cornernumber
                                , PS.preview AS s_preview                            
                                , PS.title AS s_title
                                , PS.searchkeyword AS s_searchkeyword
                                , PS.actor AS s_actor
                                , PS.targetage AS s_targetage
                                , PS.genre AS s_genre
                                , DATE_FORMAT(PS.insert_time, '%Y\\%m\\%d') As archive_date
                                , C.clip_pk
                                , DATE_FORMAT(C.broaddate, '%Y%m%d') AS broaddate
                                , DATE_FORMAT(C.edit_date, '%Y%m%d') AS edit_date
                                , c.userid
                                , C.gid                            
                                , C.contentid                                                                
                                , C.clipid
                                , C.cid
                                , C.cliporder
                                , C.title
                                , C.synopsis
                                , C.searchkeyword
                                , C.mediadomain
                                , C.filepath
                                , C.itemtypeid
                                , C.cliptype
                                , C.clipcategory
                                , C.clipcategory_name
                                , C.subcategory
                                , C.contentimg
                                , C.imgsrcpath
                                , C.clipsrcpath
                                , C.orgimgname
                                , C.orgclipname
                                , C.playtime
                                , '0' AS starttime
                                , '0' AS endtime
                                , C.returnlink
                                , C.targetage
                                , C.targetnation
                                , C.targetplatform
                                , C.limitnation                                
                                , C.isuse
                                , C.userid
                                , C.channelid
                                , C.hashtag
                                , DATE_FORMAT(C.filemodifydate, '%Y%m%d%H%i%s') AS filemodifydate
                                , C.reservedate As reservedate
                                , C.linktitle1
                                , C.linkurl1
                                , C.linktitle2
                                , C.linkurl2
                                , C.linktitle3
                                , C.linkurl3
                                , C.linktitle4
                                , C.linkurl4
                                , C.linktitle5
                                , C.linkurl5
                                , C.isfullvod
                                , C.targetplatformvalue
                                , DATE_FORMAT(C.broaddate, '%Y%m%d') AS broaddate
                                , C.sportscomment
                                , C.platformisuse
                                , C.masterclipyn
                                , C.cdnurl_img
                                , C.cdnurl_mov
                                , C.cdnurl_mov_T1
                                , C.cdnurl_mov_T2
                                , C.v_codec
                                , C.v_bitrate
                                , C.v_format
                                , C.v_resol_x
                                , C.v_resol_y
                                , C.v_profile
                                , C.v_cabac
                                , C.v_gop
                                , C.v_version
                                , C.a_codec
                                , C.a_bitrate
                                , C.status
                                , C.ftp_target
                                , C.actor as actor
                                , IFNULL(C.rtmp_url, '') AS rtmp_url
                                , IFNULL(C.rtmp_url_T1, '') AS rtmp_url_T1
                                , IFNULL(C.rtmp_url_T2, '') AS rtmp_url_T2
                                , IFNULL(C.hls_url, '') AS hls_url
                                , IFNULL(C.hls_url_T1, '') AS hls_url_T1
                                , IFNULL(C.hls_url_T2, '') AS hls_url_T2                                
                                FROM TB_CLIP C
                                INNER JOIN TB_PROGRAM_SEQ PS ON C.gid = PS.gid
                                WHERE 1=1
                                AND C.status = 'Ready'
                                LIMIT 0,1");
            connPool.ConnectionOpen();
            //MySqlTransaction trans = connPool.getConnection().BeginTransaction();
            MySqlDataAdapter adpt = new MySqlDataAdapter(m_sql, connPool.getConnection());
            adpt.Fill(ds, "CLIP");
            //trans.Commit();
            connPool.ConnectionClose();
            return true;
        }

        public bool GetCopyClipService(DataSet ds)
        {            
            m_sql = String.Format(@"
                        SELECT
                         A.archive_pk
                         , IFNULL(A.edit_count_tail, '') as edit_count_tail
                         , C.clip_pk AS clip_pk
                         , C.cid AS cid
                         , C.gid AS gid
                         , C.ftp_target AS ftp_target
                         , DATE_FORMAT(C.broaddate,'%Y%m%d') AS broaddate
                         , DATE_FORMAT(A.starttime, '%Y-%m-%d %H:%i:%s') AS starttime
                         , DATE_FORMAT(A.endtime, '%Y-%m-%d %H:%i:%s') AS endtime
                         , A.srcpath AS srcpath
                         , A.targetpath AS dstpath
                         , A.status as status
                         , A.type as type
                         , YT.videoid as yt_videoid
                         , DM.videoid as dm_videoid                         
                         FROM TB_ARCHIVE A
                         INNER JOIN TB_CLIP C ON C.cid = A.cid
                         LEFT JOIN TB_YOUTUBE YT ON YT.cid = C.cid
                         LEFT JOIN TB_DAILYMOTION DM ON DM.cid = C.cid
                         WHERE A.status = 'Pending'
                         ORDER BY A.archive_pk ASC
                         LIMIT 0,1");
            connPool.ConnectionOpen();
            //MySqlTransaction trans = connPool.getConnection().BeginTransaction();
            MySqlDataAdapter adpt = new MySqlDataAdapter(m_sql, connPool.getConnection());
            adpt.Fill(ds, "COPY");
            //trans.Commit();                
            connPool.ConnectionClose();
            return true;
        }

        public bool GetProgramSeqService(DataSet ds)
        {            
            m_sql = String.Format(@"SELECT
                                program_seq_pk
                                , DATE_FORMAT(onair_date, '%Y%m%d') AS onair_date
                                , DATE_FORMAT(edit_date, '%Y%m%d') as edit_date
                                , userid                                 
                                , contentid
                                , originid
                                , DATE_FORMAT(phun_onair_ymd, '%Y%m%d') AS phun_onair_ymd
                                , cornerid
                                , smr_pid                                
                                , programid
                                , contentnumber
                                , cornernumber
                                , preview
                                , DATE_FORMAT(broaddate, '%Y%m%d') as broaddate
                                , title
                                , contentimg
                                , imgsrcpath
                                , orgimgname
                                , searchkeyword
                                , actor
                                , targetage
                                , targetnation
                                , targetplatform
                                , limitnation
                                , platformisuse
                                , genre
                                , isuse
                                , tempyn
                                , phun_ch
                                , phun_ps
                                , phun_case
                                , phun_pgm_seq
                                , cdnurl_img
                                , STATUS FROM TB_PROGRAM_SEQ
                                WHERE 1=1
                                AND status = 'Ready'
                                LIMIT 0,1");
            connPool.ConnectionOpen();
            //MySqlTransaction trans = connPool.getConnection().BeginTransaction();
            MySqlDataAdapter adpt = new MySqlDataAdapter(m_sql, connPool.getConnection());
            adpt.Fill(ds, "PROGRAM_SEQ");
            //trans.Commit();
            connPool.ConnectionClose();
            return true;
        }

        public bool UpdateArchiveServiceRunning(String gid)
        {            
            m_sql = String.Format("UPDATE TB_PROGRAM_SEQ SET starttime = CURRENT_TIMESTAMP(), status = 'Archive' WHERE gid = '{0}'", gid);
            connPool.ConnectionOpen();
            //Running 으로 변경
            cmd = new MySqlCommand(m_sql, connPool.getConnection());
            cmd.ExecuteNonQuery();
            connPool.ConnectionClose();
            return true;
        }

        public bool UpdateProgramSeqStatus(String gid, String status)
        {
            
            m_sql = String.Format("UPDATE TB_PROGRAM_SEQ SET status = '{1}' WHERE gid = '{0}'", gid, status);            

            connPool.ConnectionOpen();
            cmd = new MySqlCommand(m_sql, connPool.getConnection());
            cmd.ExecuteNonQuery();
            connPool.ConnectionClose();
            return true;
        }

        public bool UpdateProgramSeqStatus(String gid, String status, String errmsg)
        {
            if ( status.ToLower() == "failed")
            {
                m_sql = String.Format("UPDATE TB_PROGRAM_SEQ SET status = '{1}', errmsg = '{2}' WHERE gid = '{0}'", gid, status, errmsg);
            } else
            {
                m_sql = String.Format("UPDATE TB_PROGRAM_SEQ SET status = '{1}' WHERE gid = '{0}'", gid, status);
            }

            connPool.ConnectionOpen();
            cmd = new MySqlCommand(m_sql, connPool.getConnection());
            cmd.ExecuteNonQuery();
            connPool.ConnectionClose();
            return true;                    
        }

        public bool UpdateFromYtMeta(YTMetaInfo ytMetaInfo)
        {
            connPool.ConnectionOpen();
            m_sql = String.Format(@"UPDATE TB_YOUTUBE
                                SET title = {0}
                                , description = {1}
                                , tag = {2}
                                , isuse = '{3}'
                                , category = '{4}'
                                , start_time = CONVERT_TZ('{5}', '+00:00', '+09:00')
                                , spoken_language = '{6}'
                                , target_language = '{7}'
                                , trans_lang_title = {8}
                                , trans_lang_desc = {9}
                                , start_time = '{10}'
                                WHERE videoid = '{11}'"
                                , "@title"
                                , "@description"
                                , "@tag"
                                , ytMetaInfo.privacy
                                , ytMetaInfo.category                                
                                , ytMetaInfo.start_time
                                , ytMetaInfo.spoken_language
                                , ytMetaInfo.target_language
                                , "@trans_lang_title"
                                , "@trans_lang_desc"
                                , ytMetaInfo.start_time
                                , ytMetaInfo.videoid);            
            cmd = new MySqlCommand(m_sql, connPool.getConnection());
            cmd.Parameters.Add(new MySqlParameter("@title", ytMetaInfo.title));
            cmd.Parameters.Add(new MySqlParameter("@tag", ytMetaInfo.tag));
            cmd.Parameters.Add(new MySqlParameter("@description", ytMetaInfo.description));
            cmd.Parameters.Add(new MySqlParameter("@trans_lang_title", ytMetaInfo.trans_lang_title));
            cmd.Parameters.Add(new MySqlParameter("@trans_lang_desc", ytMetaInfo.trans_lang_desc));            

            cmd.ExecuteNonQuery();
            connPool.ConnectionClose();
            return true;
        }

        public bool UpdateYoutubeReady(String cid)
        {
            connPool.ConnectionOpen();
            m_sql = String.Format("UPDATE TB_YOUTUBE SET status = 'Ready' WHERE cid = '{0}'", cid);
            //Running 으로 변경
            cmd = new MySqlCommand(m_sql, connPool.getConnection());
            cmd.ExecuteNonQuery();
            connPool.ConnectionClose();
            return true;
        }

        public bool UpdateYoutubeSessionID(String cid, String sessionID)
        {
            connPool.ConnectionOpen();
            m_sql = String.Format("UPDATE TB_YOUTUBE SET session_id = '{1}' WHERE cid = '{0}'", cid, sessionID);
            //Running 으로 변경
            cmd = new MySqlCommand(m_sql, connPool.getConnection());
            cmd.ExecuteNonQuery();
            connPool.ConnectionClose();
            return true;
        }

        public bool UpdateYoutubeVideoIDToNULL(String videoid)
        {
            connPool.ConnectionOpen();
            m_sql = String.Format("UPDATE TB_YOUTUBE SET videoid = NULL WHERE videoid = '{0}'", videoid);
            //Running 으로 변경
            cmd = new MySqlCommand(m_sql, connPool.getConnection());
            cmd.ExecuteNonQuery();
            connPool.ConnectionClose();
            return true;
        }

        public String GetYoutubeSessionID(String cid)
        {
            DataSet ds = new DataSet();
            String session_id = null;
            connPool.ConnectionOpen();
            m_sql = String.Format("SELECT session_id FROM TB_YOUTUBE WHERE cid = '{0}'", cid);
            MySqlDataAdapter adpt = new MySqlDataAdapter(m_sql, connPool.getConnection());
            adpt.Fill(ds, "GET_SID");
            connPool.ConnectionClose();

            if (ds.Tables[0].Rows.Count > 0)
            {
                session_id = ds.Tables[0].Rows[0]["session_id"].ToString();
            }
            
            return session_id;
        }

        public String GetThumbnail(String cid)
        {
            DataSet ds = new DataSet();
            String thumbnail_img = null;
            connPool.ConnectionOpen();
            m_sql = String.Format("SELECT cdnurl_img FROM TB_CLIP WHERE cid = '{0}'", cid);
            MySqlDataAdapter adpt = new MySqlDataAdapter(m_sql, connPool.getConnection());
            adpt.Fill(ds, "GET_CLIP_IMG");
            connPool.ConnectionClose();
            try
            {
                thumbnail_img = ds.Tables[0].Rows[0]["cdnurl_img"].ToString();
            }
            catch
            {
                thumbnail_img = null;
            }

            return thumbnail_img;
        }

        public String GetCaption(String cid)
        {
            DataSet ds = new DataSet();
            String thumbnail_caption = null;
            connPool.ConnectionOpen();
            m_sql = String.Format("SELECT cdnurl_subtitle FROM TB_CLIP WHERE cid = '{0}'", cid);
            MySqlDataAdapter adpt = new MySqlDataAdapter(m_sql, connPool.getConnection());
            adpt.Fill(ds, "GET_CLIP_CAPTION");
            connPool.ConnectionClose();
            try
            {
                thumbnail_caption = ds.Tables[0].Rows[0]["cdnurl_subtitle"].ToString();
            }
            catch
            {
                thumbnail_caption = null;
            }
            return thumbnail_caption;
        }

        public bool UpdateDailymotionReady(String cid)
        {
            connPool.ConnectionOpen();
            m_sql = String.Format("UPDATE TB_DAILYMOTION SET status = 'Ready' WHERE cid = '{0}'", cid);
            //Running 으로 변경
            cmd = new MySqlCommand(m_sql, connPool.getConnection());
            cmd.ExecuteNonQuery();
            connPool.ConnectionClose();
            return true;
        }

        public bool UpdateSendingProgress(String pk, double percent )
        {
            m_sql = String.Format(@"UPDATE TB_FTP_QUEUE SET percent = '{0}' WHERE ftp_queue_pk = '{1}'", percent, pk);
            connPool.ConnectionOpen();
            MySqlCommand cmd = new MySqlCommand(m_sql, connPool.getConnection());
            cmd.ExecuteNonQuery();
            connPool.ConnectionClose();
            return true;
        }

        public bool UpdateArchiveProgramServiceRunning(String pid)
        {
            connPool.ConnectionOpen();
            m_sql = String.Format("UPDATE TB_PROGRAM SET job_starttime = CURRENT_TIMESTAMP(), status = 'Archive' WHERE pid = '{0}'", pid);
            //Running 으로 변경
            cmd = new MySqlCommand(m_sql, connPool.getConnection());
            cmd.ExecuteNonQuery();
            connPool.ConnectionClose();
            return true;
        }

        public bool SetArchiveSmrProgramServiceRunning(String pid)
        {
            connPool.ConnectionOpen();
            m_sql = String.Format("UPDATE TB_SMR_PROGRAM SET edit_time = CURRENT_TIMESTAMP(), status = 'Archive' WHERE pid = '{0}'", pid);
            //Running 으로 변경
            cmd = new MySqlCommand(m_sql, connPool.getConnection());
            cmd.ExecuteNonQuery();
            connPool.ConnectionClose();
            return true;
        }

        public bool UpdateFtpCompleted(String pk)
        {
            m_sql = String.Format("UPDATE TB_FTP_QUEUE SET endtime = CURRENT_TIMESTAMP(), status = 'Completed', percent = 100 WHERE ftp_queue_pk = '{0}'", pk);

            connPool.ConnectionOpen();
            cmd = new MySqlCommand(m_sql, connPool.getConnection());
            cmd.ExecuteNonQuery();
            connPool.ConnectionClose();
            return true;
        }

        /*
        public bool UpdateFtpFailed(String pk)
        {
            m_sql = String.Format("UPDATE TB_FTP_QUEUE SET endtime = CURRENT_TIMESTAMP(), status = 'Failed', percent = 100 WHERE ftp_queue_pk = '{0}'", pk);

            connPool.ConnectionOpen();
            cmd = new MySqlCommand(m_sql, connPool.getConnection());
            cmd.ExecuteNonQuery();
            connPool.ConnectionClose();
            return true;
        }
        */

        public bool UpdateFtpStatus(String pk, String status)
        {
            if (status.ToLower() == "failed" || status.ToLower() == "completed")
            {
                m_sql = String.Format("UPDATE TB_FTP_QUEUE SET endtime = CURRENT_TIMESTAMP(), status = '{1}', endtime = CURRENT_TIMESTAMP() WHERE ftp_queue_pk = '{0}'", pk, status);
            }
            else
            {
                m_sql = String.Format("UPDATE TB_FTP_QUEUE SET endtime = CURRENT_TIMESTAMP(), status = '{1}', WHERE ftp_queue_pk = '{0}'", pk, status);
            }            

            connPool.ConnectionOpen();
            cmd = new MySqlCommand(m_sql, connPool.getConnection());
            cmd.ExecuteNonQuery();
            connPool.ConnectionClose();
            return true;
        }

        public bool UpdateFtpStatus(String pk, String status, String errmsg)
        {
            if ( status.ToLower() == "failed")
            {
                m_sql = String.Format("UPDATE TB_FTP_QUEUE SET endtime = CURRENT_TIMESTAMP(), status = '{1}', errmsg = '{2}', endtime = CURRENT_TIMESTAMP() WHERE ftp_queue_pk = '{0}'", pk, status, errmsg);
            } else
            {
                m_sql = String.Format("UPDATE TB_FTP_QUEUE SET endtime = CURRENT_TIMESTAMP(), status = '{1}', errmsg = '{2}' WHERE ftp_queue_pk = '{0}'", pk, status, errmsg);
            }            

            connPool.ConnectionOpen();
            cmd = new MySqlCommand(m_sql, connPool.getConnection());
            cmd.ExecuteNonQuery();
            connPool.ConnectionClose();
            return true;
        }

        public bool UpdateDMVideoid(String cid, String videoid)
        {
            m_sql = String.Format("UPDATE TB_DAILYMOTION SET videoid = '{0}' WHERE cid = '{1}'", videoid, cid);
            connPool.ConnectionOpen();
            cmd = new MySqlCommand(m_sql, connPool.getConnection());
            cmd.ExecuteNonQuery();
            connPool.ConnectionClose();
            return true;
        }

        public bool InsertDMChannelList(DMChannelList dmList)
        {
            m_sql = String.Format(@"INSERT INTO TB_DM_CHANNELLIST (id, name, description) VALUES('{0}','{1}','{2}')
                                    ON DUPLICATE KEY UPDATE id = '{0}', name = '{1}', description = '{2}'", dmList.id, dmList.name, dmList.description);
            connPool.ConnectionOpen();
            cmd = new MySqlCommand(m_sql, connPool.getConnection());
            cmd.ExecuteNonQuery();
            connPool.ConnectionClose();
            return true;
        }

        public bool UpdateProgramImgCompleted(String full_url, String pid, String img_type)
        {
            String target_field = "";
            if (img_type == "1")
            {
                target_field = "cdnurl_img";
            }
            else if (img_type == "2")
            {
                target_field = "cdn_poster_img";
            }
            else if (img_type == "3")
            {
                target_field = "cdn_thumb_img";
            }
            else if (img_type == "4")
            {
                target_field = "cdn_circle_img";
            }            
            connPool.ConnectionOpen();
            m_sql = String.Format(@"UPDATE TB_PROGRAM
                                    SET {2} = '{0}', status = 'Completed'
                                    WHERE pid = '{1}'", full_url, pid, target_field);
            cmd = new MySqlCommand(m_sql, connPool.getConnection());
            cmd.ExecuteNonQuery();
            connPool.ConnectionClose();
            return true;
        }

        public bool UpdateSmrProgramImgCompleted(String full_url, String pid, String img_type)
        {
            String target_field = "";
            if (img_type == "1")
            {
                target_field = "cdn_img";
            }
            else if (img_type == "2")
            {
                target_field = "cdn_posterimg1";
            }
            else if (img_type == "3")
            {
                target_field = "cdn_posterimg2";
            }
            else if (img_type == "4")
            {
                target_field = "cdn_bannerimg";
            }
            else if (img_type == "5")
            {
                target_field = "cdn_thumbimg";
            }
            connPool.ConnectionOpen();
            m_sql = String.Format(@"UPDATE TB_SMR_PROGRAM
                                    SET {2} = '{0}', status = 'Completed'
                                    WHERE pid = '{1}'", full_url, pid, target_field);
            cmd = new MySqlCommand(m_sql, connPool.getConnection());
            cmd.ExecuteNonQuery();
            connPool.ConnectionClose();
            return true;
        }

        public bool UpdateProgramSeqImg(String full_url, String gid)
        {
            connPool.ConnectionOpen();
            m_sql = String.Format(@"UPDATE TB_PROGRAM_SEQ
                                    SET cdnurl_img = '{0}'
                                    WHERE gid = '{1}'", full_url, gid);
            cmd = new MySqlCommand(m_sql, connPool.getConnection());
            cmd.ExecuteNonQuery();
            connPool.ConnectionClose();
            return true;
        }

        public bool UpdateContentBypass(String pk)
        {
            m_sql = String.Format(@"UPDATE TB_FTP_QUEUE SET starttime = CURRENT_TIMESTAMP(), endtime = CURRENT_TIMESTAMP(), status = 'Bypass' WHERE ftp_queue_pk = '{0}'", pk);
            connPool.ConnectionOpen();
            cmd = new MySqlCommand(m_sql, connPool.getConnection());
            cmd.ExecuteNonQuery();
            connPool.ConnectionClose();
            return true;
        }

        public bool UpdateAliasFilepath(String pk, String filepath, String filename)
        {
            m_sql = String.Format("UPDATE TB_FTP_QUEUE SET targetpath = '{1}', targetfilename = '{2}' WHERE ftp_queue_pk = '{0}'", pk, filepath, filename);
            connPool.ConnectionOpen();
            cmd = new MySqlCommand(m_sql, connPool.getConnection());
            cmd.ExecuteNonQuery();
            connPool.ConnectionClose();
            return true;
        }

        public bool UpdateFTPFilepath(String pk, String filepath, String filename)
        {
            m_sql = String.Format("UPDATE TB_FTP_QUEUE SET targetpath = '{1}', targetfilename = '{2}' WHERE ftp_queue_pk = '{0}'", pk, filepath, filename);
            connPool.ConnectionOpen();
            cmd = new MySqlCommand(m_sql, connPool.getConnection());
            cmd.ExecuteNonQuery();
            connPool.ConnectionClose();
            return true;
        }

        public bool UpdateCallbackURL(String pk, String full_url, String rtmp_url, String hls_url)
        {
            connPool.ConnectionOpen();
            m_sql = String.Format(@"UPDATE TB_CALLBACK SET endtime = CURRENT_TIMESTAMP()
                                , status = 'Completed'
                                , download_url = '{1}'
                                , rtmp_url = '{2}'
                                , hls_url = '{3}'
                                WHERE callback_pk = '{0}'", pk, full_url, rtmp_url, hls_url);
            //Completed 로 변경
            cmd = new MySqlCommand(m_sql, connPool.getConnection());
            cmd.ExecuteNonQuery();
            connPool.ConnectionClose();
            return true;
        }

        public bool UpdateTranscodePK(String tc_pk, String ftp_pk)
        {
            m_sql = String.Format("UPDATE TB_FTP_QUEUE SET tc_pk = '{0}' WHERE ftp_queue_pk = '{1}'", tc_pk, ftp_pk);
            connPool.ConnectionOpen();
            cmd = new MySqlCommand(m_sql, connPool.getConnection());
            cmd.ExecuteNonQuery();
            connPool.ConnectionClose();
            return true;
        }

        /*public bool UpdateClipReady(String pk)
        {
            connPool.ConnectionOpen();
            m_sql = String.Format(@"UPDATE TB_CLIP
                                    SET status = 'Ready'
                                    WHERE clip_pk = '{0}'", pk);
            cmd = new MySqlCommand(m_sql, connPool.getConnection());
            cmd.ExecuteNonQuery();
            connPool.ConnectionClose();
            return true;
        }*/

        public bool UpdateClipStatus(String cid, String status)
        {            
            connPool.ConnectionOpen();
            m_sql = String.Format(@"UPDATE TB_CLIP
                            SET status = '{1}'
                            WHERE cid = '{0}'", cid, status);
            cmd = new MySqlCommand(m_sql, connPool.getConnection());
            cmd.ExecuteNonQuery();
            connPool.ConnectionClose();
            logger.logging(String.Format("({0}) clip Status {1}", cid, status));                
            
            return true;
        }

        public bool UpdateYoutubeStatus(String cid, String status)
        {
            connPool.ConnectionOpen();
            m_sql = String.Format(@"UPDATE TB_YOUTUBE
                            SET status = '{1}'
                            WHERE cid = '{0}'", cid, status);
            cmd = new MySqlCommand(m_sql, connPool.getConnection());
            cmd.ExecuteNonQuery();
            connPool.ConnectionClose();
            logger.logging(String.Format("({0}) youtube Status {1}", cid, status));

            return true;
        }

        public bool UpdateDailyMotionStatus(String cid, String status)
        {
            connPool.ConnectionOpen();
            m_sql = String.Format(@"UPDATE TB_DAILYMOTION
                            SET status = '{1}'
                            WHERE cid = '{0}'", cid, status);
            cmd = new MySqlCommand(m_sql, connPool.getConnection());
            cmd.ExecuteNonQuery();
            connPool.ConnectionClose();
            logger.logging(String.Format("({0}) dailymotion Status {1}", cid, status));

            return true;
        }

        public bool UpdateClipStatus(String cid, String status, String errmsg)
        {
            if (status.ToLower() == "failed")
            {

                m_sql = String.Format(@"UPDATE TB_CLIP
                            SET status = '{1}'
                            , errmsg = '{2}'
                            WHERE cid = '{0}'", cid, status, errmsg);
            } else
            {
                m_sql = String.Format(@"UPDATE TB_CLIP
                            SET status = '{1}'                            
                            WHERE cid = '{0}'", cid, status);
            }

            connPool.ConnectionOpen();
            cmd = new MySqlCommand(m_sql, connPool.getConnection());
            cmd.ExecuteNonQuery();
            connPool.ConnectionClose();
            logger.logging(String.Format("({0}) clip Status {1}", cid, status));

            return true;
        }

        public bool UpdateClipInfos(String cid, MediaInfomation mediainfo)
        {
            connPool.ConnectionOpen();
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
                                                    WHERE cid = '{7}'"
                                                    , mediainfo.duration
                                                    , mediainfo.v_bitrate
                                                    , mediainfo.v_codec
                                                    , mediainfo.v_resolution_x
                                                    , mediainfo.v_resolution_y
                                                    , mediainfo.a_bitrate
                                                    , mediainfo.a_codec
                                                    , cid
                                                    , mediainfo.v_profile
                                                    , mediainfo.v_gop
                                                    , mediainfo.v_cabac
                                                    , mediainfo.v_format
                                                    , mediainfo.v_version
                                                    , mediainfo.filesize);
            cmd = new MySqlCommand(m_sql, connPool.getConnection());
            cmd.ExecuteNonQuery();
            connPool.ConnectionClose();
            return true;
        }

        public bool CheckYoutubeReady(String cid)
        {
            DataSet ds = new DataSet();
            connPool.ConnectionOpen();
            m_sql = String.Format(@"SELECT
                                     YT.status as status                                                                   
                                     FROM TB_YOUTUBE YT
                                     WHERE 1=1                                     
                                     AND YT.cid = '{0}'", cid);
            MySqlDataAdapter adpt = new MySqlDataAdapter(m_sql, connPool.getConnection());
            adpt.Fill(ds, "YT_READY");
            connPool.ConnectionClose();

            if (ds.Tables[0].Rows.Count > 0)
            {
                String status = ds.Tables[0].Rows[0]["status"].ToString();                

                if ( status == "Ready" )
                {
                    logger.logging(String.Format("({0}) Youtube Ready", cid));
                    ds.Clear();
                    return true;
                }
            }
            ds.Clear();
            return false;
        }

        public bool UpdateYoutubeStatus(String cid, String status, YoutubeID ytID)
        {
            connPool.ConnectionOpen();
            m_sql = String.Format(@"UPDATE TB_YOUTUBE
                                    SET status = '{0}', assetid = '{1}', videoid = '{2}', session_id = ''                                
                                    WHERE cid = '{3}'", status, ytID.AssetID, ytID.VideoID, cid);
            cmd = new MySqlCommand(m_sql, connPool.getConnection());
            cmd.ExecuteNonQuery();
            connPool.ConnectionClose();
            return true;
        }       

        public bool UpdateCDNURL(String type, String full_url, String pk)
        {
            if (type.ToLower().Equals("srt"))
            {
                type = "subtitle";
            }
            type = type.ToLower();
            m_sql = String.Format(@"UPDATE TB_CLIP
                                    SET cdnurl_{0} = '{1}'                                   
                                    WHERE clip_pk = '{2}'"
                                    , type
                                    , full_url
                                    , pk);
            connPool.ConnectionOpen();
            cmd = new MySqlCommand(m_sql, connPool.getConnection());
            cmd.ExecuteNonQuery();
            connPool.ConnectionClose();
            return true;
        }

        public bool Update_YT_CDNURL(String type, String full_url, String cid)
        {            
            type = type.ToLower();
            if ( type == "yt_img")
            {
                type = "img";
            } else if ( type == "yt_srt")
            {
                type = "srt";
            }
            m_sql = String.Format(@"UPDATE TB_YOUTUBE
                                    SET cdn_{0} = '{1}'                                   
                                    WHERE cid = '{2}'"
                                    , type
                                    , full_url
                                    , cid);
            connPool.ConnectionOpen();
            cmd = new MySqlCommand(m_sql, connPool.getConnection());
            cmd.ExecuteNonQuery();
            connPool.ConnectionClose();
            return true;
        }

        public bool Update_YT_ITEM_CDNURL(String full_url, String cid, String language)
        {            
            m_sql = String.Format(@"UPDATE TB_YT_ITEMS
                                    SET cdn_path = '{0}'                                   
                                    WHERE cid = '{1}' AND language = '{2}'"                                
                                    , full_url
                                    , cid
                                    , language
                                    );
            connPool.ConnectionOpen();
            cmd = new MySqlCommand(m_sql, connPool.getConnection());
            cmd.ExecuteNonQuery();
            connPool.ConnectionClose();
            return true;
        }

        public bool UpdateClipInfos(String full_url, String rtmp_url, String hls_url, String pk)
        {
            m_sql = String.Format(@"UPDATE TB_CLIP
                                    SET cdnurl_mov = '{0}'
                                    , rtmp_url = '{1}'
                                    , hls_url = '{2}'                                    
                                    WHERE clip_pk = '{3}'", full_url, rtmp_url, hls_url, pk);
            connPool.ConnectionOpen();
            cmd = new MySqlCommand(m_sql, connPool.getConnection());
            cmd.ExecuteNonQuery();
            connPool.ConnectionClose();
            return true;
        }

        public bool UpdateClipArchivePath(String path, String cid, String lang)
        {
            connPool.ConnectionOpen();
            m_sql = String.Format(@"UPDATE TB_YT_ITEMS SET archive_path = '{0}' WHERE cid = '{1}' AND language = '{2}'", path, cid, lang);
            cmd = new MySqlCommand(m_sql, connPool.getConnection());
            cmd.ExecuteNonQuery();
            connPool.ConnectionClose();
            return true;
        }

        public bool CheckUrlisCompleted(String pk)
        {
            DataSet ds = new DataSet();
            connPool.ConnectionOpen();
            m_sql = String.Format(@"SELECT
                                     C.cdnurl_img,
                                     C.cdnurl_mov                                     
                                     FROM TB_CLIP C
                                     WHERE 1=1                                     
                                     AND clip_pk = '{0}'", pk);            
            MySqlDataAdapter adpt = new MySqlDataAdapter(m_sql, connPool.getConnection());
            adpt.Fill(ds, "CHECK_URL");            
            connPool.ConnectionClose();

            if (ds.Tables[0].Rows.Count > 0)
            {
                String img_url = ds.Tables[0].Rows[0]["cdnurl_img"].ToString();
                String mov_url = ds.Tables[0].Rows[0]["cdnurl_mov"].ToString();
                
                if (!String.IsNullOrEmpty(img_url) && !String.IsNullOrEmpty(mov_url))
                {
                    ds.Clear();
                    return true;
                }                
            }
            ds.Clear();
            return false;
        }

        public Boolean YTimgUploadCheck(String cid, out String ImgFileName)
        {
            ImgFileName = null;
            DataSet ds = new DataSet();
            String status = null;            
            connPool.ConnectionOpen();
            m_sql = String.Format(@"SELECT
                                    status
                                    , targetfilename
                                    FROM TB_FTP_QUEUE
                                    WHERE 1=1
                                    AND cid = '{0}'
                                    AND customer_id = 9
                                    AND TYPE = 'IMG'
                                    ORDER BY starttime DESC
                                    LIMIT 0,1
                                     ", cid);
            MySqlDataAdapter adpt = new MySqlDataAdapter(m_sql, connPool.getConnection());
            adpt.Fill(ds, "CHECK_YT_IMG");
            connPool.ConnectionClose();            

            if (ds.Tables[0].Rows.Count > 0)
            {
                status = ds.Tables[0].Rows[0]["status"].ToString();
                if (String.Equals(status, "Completed"))
                {
                    ImgFileName = String.Format("{0}{1}", cid, Path.GetExtension(ds.Tables[0].Rows[0]["targetfilename"].ToString()));
                    ds.Clear();
                    return true;
                }
            }
            return false;
        }

        public Boolean YTUploadCheck(String cid, String type, out String targetFaileName)
        {
            if ( type.ToLower().Equals("mov") )
            {
                type = "archive_clip";
            }
            else if ( type.ToLower().Equals("srt"))
            {
                type = "archive_subtitle";
            }
            else if ( type.ToLower().Equals("img"))
            {
                type = "archive_img";
            }

            targetFaileName = null;
            DataSet ds = new DataSet();            
            connPool.ConnectionOpen();
            m_sql = String.Format(@"SELECT
                                    {0}                                    
                                    FROM TB_CLIP
                                    WHERE 1=1
                                    AND cid = '{1}'
                                     ", type, cid);
            MySqlDataAdapter adpt = new MySqlDataAdapter(m_sql, connPool.getConnection());
            adpt.Fill(ds, "CHECK_YT");
            connPool.ConnectionClose();

            if (ds.Tables[0].Rows.Count > 0)
            {
                targetFaileName = ds.Tables[0].Rows[0][type].ToString();
                logger.logging("UploadFile Check : " + targetFaileName);
                if (!String.IsNullOrEmpty(targetFaileName))
                {
                    targetFaileName = String.Format("{0}{1}", cid, Path.GetExtension(targetFaileName));
                    ds.Clear();
                    return true;
                }
                
            }
            return false;
        }

        public bool DeleteEpgInfo(String day, String ch_no)
        {
            connPool.ConnectionOpen();
            m_sql = String.Format(@"DELETE FROM TB_EPG_INFO WHERE startymd = '{0}'
                                    AND ch_no = '{1}'", day, ch_no);
            cmd = new MySqlCommand(m_sql, connPool.getConnection());
            cmd.ExecuteNonQuery();
            connPool.ConnectionClose();
            return true;
        }

        public bool InsertEpginfo(vo.EPGInfo epgInfo)
        {
            connPool.ConnectionOpen();
            m_sql = String.Format(@"INSERT INTO TB_EPG_INFO(sid, mid, ch_no, ch_name, program_name, program_subname, startymd, starttime, endtime, frequency, hd_YN, grade, duration, suwna_YN)
                                                  VALUES ('{0}', '{1}', '{2}', '{3}', {4}, {5}, '{6}', '{7}', '{8}', '{9}', '{10}', '{11}', '{12}', '{13}' )
                                                  ON DUPLICATE KEY UPDATE program_name = {4}_u, program_subname = {5}_u, startymd = '{6}', starttime = '{7}', endtime = '{8}'", epgInfo.SID
                                                                                                                                   , epgInfo.MID
                                                                                                                                   , epgInfo.ch_no
                                                                                                                                   , epgInfo.ch_name
                                                                                                                                   , "@program_name"
                                                                                                                                   , "@program_subname"
                                                                                                                                   , epgInfo.StartYMD
                                                                                                                                   , epgInfo.StartTime
                                                                                                                                   , epgInfo.EndTime
                                                                                                                                   , epgInfo.Frequency
                                                                                                                                   , epgInfo.HD
                                                                                                                                   , epgInfo.Grade
                                                                                                                                   , epgInfo.Duration
                                                                                                                                   , epgInfo.Suwha);
            cmd = new MySqlCommand(m_sql, connPool.getConnection());
            cmd.Parameters.Add(new MySqlParameter("@program_name", epgInfo.ProgramName));
            cmd.Parameters.Add(new MySqlParameter("@program_subname", epgInfo.ProgramName));
            cmd.Parameters.Add(new MySqlParameter("@program_name_u", epgInfo.ProgramName));
            cmd.Parameters.Add(new MySqlParameter("@program_subname_u", epgInfo.ProgramSubName));
            cmd.ExecuteNonQuery();
            connPool.ConnectionClose();
            return true;
        }

        public bool WriteLockTable(String tablename)
        {
            connPool.ConnectionOpen();
            m_sql = String.Format("LOCK TABLES {0} WRITE", tablename);            
            cmd = new MySqlCommand(m_sql, connPool.getConnection());
            cmd.ExecuteNonQuery();
            connPool.ConnectionClose();
            return true;
        }

        public bool UNLOCKTable()
        {
            connPool.ConnectionOpen();
            m_sql = String.Format("UNLOCK TABLES");            
            cmd = new MySqlCommand(m_sql, connPool.getConnection());
            cmd.ExecuteNonQuery();
            connPool.ConnectionClose();
            return true;
        }
    }
}