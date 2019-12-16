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

        public bool WaitYoutubeResponse(DataSet ds)
        {
            connPool.ConnectionOpen();
            m_sql = String.Format(@"SELECT
                                    cid
                                    , status
                                    , spoken_language
                                    , target_language
                                    , org_lang_title
                                    , org_lang_desc
                                    , trans_lang_title
                                    , trans_lang_desc
                                    , session_id
                                    FROM TB_YOUTUBE YT                                    
                                    WHERE 1=1
                                    AND YT.status = 'Ready'
                                    LIMIT 0,1");
            //MySqlTransaction trans = connPool.getConnection().BeginTransaction();
            MySqlDataAdapter adpt = new MySqlDataAdapter(m_sql, connPool.getConnection());
            adpt.Fill(ds, "YT");
            //trans.Commit();
            connPool.ConnectionClose();            
            return true;
        }

        public bool GetDMReady(DataSet ds)
        {
            connPool.ConnectionOpen();
            m_sql = String.Format(@"SELECT DM.cid as cid
                                    , DM.videoid
                                    , DM.playlistid
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
                                    FROM TB_DAILYMOTION DM
                                    LEFT JOIN TB_CLIP C ON C.cid = DM.cid
                                    WHERE 1=1
                                    AND DM.status = 'Ready'
                                    LIMIT 0,1");
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

        public bool GetArchiveProgramServiceInfo(DataSet ds)
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
                                seq as pk
                                , pid as pid          
                                , imgsrcpath as imgsrcpath
                                , orgimgname as orgimgname
                                , status as status
                                FROM TB_PROGRAM
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

        public bool GetArchiveClipService(DataSet ds)
        {            
            m_sql = String.Format(@"SELECT
                                    C.clip_pk
                                    , C.imgsrcpath
                                    , C.clipsrcpath
                                    , C.orgimgname
                                    , C.orgclipname
                                    , C.gid
                                    , C.cid
                                    , C.cdnurl_img
                                    , C.cdnurl_mov
                                    , IFNULL(C.metahub_YN, 'Y') AS metahub_YN
                                    , (SELECT DATE_FORMAT(C.insert_time, '%Y\\%m\\%d') FROM TB_PROGRAM_SEQ PS WHERE PS.gid = C.gid ) AS archive_date
                                    , P.section AS section
                                    , C.status                                    
                                    , C.idolvod_YN as idolvod_YN
                                    , C.idolclip_YN as idolclip_YN
                                    , C.idolvote_YN as idolvote_YN
                                    , YT.isuse AS yt_isuse
                                    , YT.videoid as yt_videoid
                                    , DM.isuse AS dm_isuse
                                    , DM.videoid as dm_videoid                                    
                                    , C.archive_img AS archive_img
                                    , C.archive_clip AS archive_clip
                                    FROM TB_CLIP C
                                    LEFT JOIN TB_PROGRAM_SEQ PS ON PS.gid = C.gid
                                    LEFT JOIN TB_PROGRAM P ON P.pid = PS.pid                                            
                                    LEFT JOIN TB_YOUTUBE YT ON YT.cid = C.cid
                                    LEFT JOIN TB_DAILYMOTION DM ON DM.cid = C.cid                                    
                                    WHERE C.STATUS = 'Pending'
                                    LIMIT 0,1");
            connPool.ConnectionOpen();
            //MySqlTransaction trans = connPool.getConnection().BeginTransaction();
            MySqlDataAdapter adpt = new MySqlDataAdapter(m_sql, connPool.getConnection());
            adpt.Fill(ds, "ARCHIVE");
            //trans.Commit();
            connPool.ConnectionClose();
            return true;
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

            if (ds.Tables[0].Rows.Count >= 2)
            {
                logger.logging(ds.Tables[0].Rows.Count.ToString());
                ds.Clear();
                return true;                
            }
            return false;
        }

        public bool SetAdditionalTranscoding(String pk, String gid, String cid, out String srcPath)
        {
            DataSet ds = new DataSet();
            connPool.ConnectionOpen();
            m_sql = String.Format(@"SELECT targetpath FROM TB_ARCHIVE WHERE clip_pk = '{0}'
                                    AND STATUS = 'Completed'
                                    AND TYPE = 'MOV'
                                    LIMIT 0,1", pk);
            MySqlDataAdapter adpt = new MySqlDataAdapter(m_sql, connPool.getConnection());
            adpt.Fill(ds, "CHECK_URL");
            connPool.ConnectionClose();

            logger.logging(ds.Tables[0].Rows[0]["targetpath"].ToString());
            if (ds.Tables[0].Rows.Count > 0)
            {
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
            logger.logging(m_sql);
            cmd = new MySqlCommand(m_sql, connPool.getConnection());
            cmd.ExecuteNonQuery();
            connPool.ConnectionClose();
            return true;
        }

        public bool GetFtpInfo(DataSet ds, String tail)
        {
            connPool.ConnectionOpen();            
            m_sql = String.Format(@"SELECT ftp_queue_pk as pk
	                                , IFNULL(FQ.srcpath, '') as srcpath
	                                , IFNULL(FQ.targetpath, '') as targetpath
	                                , IFNULL(FQ.targetfilename, '') as targetfilename
	                                , IFNULL(FQ.clip_pk, '') as clip_pk
                                    , FQ.pid as pid
	                                , FQ.gid AS gid
                                    , FQ.cid AS cid
	                                , FQ.status AS STATUS
	                                , FQ.type AS TYPE    
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
	                                , DATE_FORMAT(C.filemodifydate, '%Y%m%d%H%m%s') AS filemodifydate
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
                                    FROM TB_FTP_QUEUE FQ
                                    LEFT JOIN TB_CUSTOMER CU ON FQ.customer_id = CU.customer_id
                                    LEFT JOIN TB_PROGRAM_SEQ PS ON FQ.gid = PS.gid
                                    LEFT JOIN TB_CLIP C ON FQ.clip_pk = C.clip_pk
                                    LEFT JOIN TB_ADD_META AM ON FQ.cid = AM.cid
                                    LEFT JOIN TB_YOUTUBE YT ON FQ.cid = YT.cid
                                    LEFT JOIN TB_DAILYMOTION DM ON FQ.cid = DM.cid
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
                frmMain.WriteLogThread(String.Format(@"[FTPService] ftp_pk({0}) is Running", pk));
            }

            return true;
        }

        public bool GetCopyPrgramSeqInfo(DataSet ds)
        {
            connPool.ConnectionOpen();
            m_sql = String.Format(@"SELECT
                                     A.archive_pk
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
                 , P.pid AS pid
                 , A.srcpath AS srcpath
                 , A.targetpath AS dstpath
                 , A.status AS STATUS
                 , A.type AS TYPE
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

        public bool GetClipService(DataSet ds)
        {
            m_sql = String.Format(@"SELECT
                                TB_PROGRAM_SEQ.contentid AS s_contentid
                                , TB_PROGRAM_SEQ.cornerid AS s_cornerid                            
                                , DATE_FORMAT(TB_PROGRAM_SEQ.phun_onair_ymd, '%Y%m%d') AS s_phun_onair_ymd
                                , TB_PROGRAM_SEQ.contentnumber AS s_contentnumber
                                , TB_PROGRAM_SEQ.cornernumber AS s_cornernumber
                                , TB_PROGRAM_SEQ.preview AS s_preview                            
                                , TB_PROGRAM_SEQ.title AS s_title
                                , TB_PROGRAM_SEQ.searchkeyword AS s_searchkeyword
                                , TB_PROGRAM_SEQ.actor AS s_actor
                                , TB_PROGRAM_SEQ.targetage AS s_targetage
                                , TB_PROGRAM_SEQ.genre AS s_genre
                                , DATE_FORMAT(TB_PROGRAM_SEQ.insert_time, '%Y\\%m\\%d') As archive_date
                                , TB_CLIP.clip_pk
                                , DATE_FORMAT(TB_CLIP.broaddate, '%Y%m%d') AS broaddate
                                , DATE_FORMAT(TB_CLIP.edit_date, '%Y%m%d') AS edit_date
                                , TB_CLIP.userid                                
                                , TB_CLIP.gid                            
                                , TB_CLIP.contentid                                                                
                                , TB_CLIP.clipid
                                , TB_CLIP.cid
                                , TB_CLIP.cliporder
                                , TB_CLIP.title
                                , TB_CLIP.synopsis
                                , TB_CLIP.searchkeyword
                                , TB_CLIP.mediadomain
                                , TB_CLIP.filepath
                                , TB_CLIP.itemtypeid
                                , TB_CLIP.cliptype
                                , TB_CLIP.clipcategory
                                , TB_CLIP.clipcategory_name
                                , TB_CLIP.subcategory
                                , TB_CLIP.contentimg
                                , TB_CLIP.imgsrcpath
                                , TB_CLIP.clipsrcpath
                                , TB_CLIP.orgimgname
                                , TB_CLIP.orgclipname
                                , TB_CLIP.playtime
                                , '0' AS starttime
                                , '0' AS endtime
                                , TB_CLIP.returnlink
                                , TB_CLIP.targetage
                                , TB_CLIP.targetnation
                                , TB_CLIP.targetplatform
                                , TB_CLIP.limitnation                                
                                , TB_CLIP.isuse
                                , TB_CLIP.userid
                                , TB_CLIP.channelid
                                , TB_CLIP.hashtag
                                , DATE_FORMAT(TB_CLIP.filemodifydate, '%Y%m%d%H%i%s') AS filemodifydate
                                , TB_CLIP.reservedate As reservedate
                                , TB_CLIP.linktitle1
                                , TB_CLIP.linkurl1
                                , TB_CLIP.linktitle2
                                , TB_CLIP.linkurl2
                                , TB_CLIP.linktitle3
                                , TB_CLIP.linkurl3
                                , TB_CLIP.linktitle4
                                , TB_CLIP.linkurl4
                                , TB_CLIP.linktitle5
                                , TB_CLIP.linkurl5
                                , TB_CLIP.isfullvod
                                , TB_CLIP.targetplatformvalue
                                , DATE_FORMAT(TB_CLIP.broaddate, '%Y%m%d') AS broaddate
                                , TB_CLIP.sportscomment
                                , TB_CLIP.platformisuse
                                , TB_CLIP.masterclipyn
                                , TB_CLIP.cdnurl_img
                                , TB_CLIP.cdnurl_mov
                                , TB_CLIP.cdnurl_mov_T1
                                , TB_CLIP.cdnurl_mov_T2
                                , TB_CLIP.v_codec
                                , TB_CLIP.v_bitrate
                                , TB_CLIP.v_format
                                , TB_CLIP.v_resol_x
                                , TB_CLIP.v_resol_y
                                , TB_CLIP.v_profile
                                , TB_CLIP.v_cabac
                                , TB_CLIP.v_gop
                                , TB_CLIP.v_version
                                , TB_CLIP.a_codec
                                , TB_CLIP.a_bitrate
                                , TB_CLIP.status
                                , TB_CLIP.ftp_target
                                , IFNULL(TB_CLIP.rtmp_url, '') AS rtmp_url
                                , IFNULL(TB_CLIP.rtmp_url_T1, '') AS rtmp_url_T1
                                , IFNULL(TB_CLIP.rtmp_url_T2, '') AS rtmp_url_T2
                                , IFNULL(TB_CLIP.hls_url, '') AS hls_url
                                , IFNULL(TB_CLIP.hls_url_T1, '') AS hls_url_T1
                                , IFNULL(TB_CLIP.hls_url_T2, '') AS hls_url_T2
                                FROM TB_CLIP
                                INNER JOIN TB_PROGRAM_SEQ ON TB_CLIP.gid = TB_PROGRAM_SEQ.gid
                                WHERE 1=1
                                AND TB_CLIP.status = 'Ready'
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
                         archive_pk
                         , TB_CLIP.clip_pk AS clip_pk
                         , TB_CLIP.cid AS cid
                         , TB_CLIP.gid AS gid
                         , TB_CLIP.ftp_target AS ftp_target
                         , DATE_FORMAT(TB_CLIP.broaddate,'%Y%m%d') AS broaddate
                         , TB_ARCHIVE.srcpath AS srcpath
                         , targetpath AS dstpath
                         , TB_ARCHIVE.status
                         , TB_ARCHIVE.type                 
                         FROM TB_ARCHIVE
                         INNER JOIN TB_CLIP ON TB_CLIP.clip_pk = TB_ARCHIVE.clip_pk
                         AND TB_ARCHIVE.status = 'Pending'
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

        public bool UpdateArchiveServiceRunning(String pk)
        {
            connPool.ConnectionOpen();
            m_sql = String.Format("UPDATE TB_PROGRAM_SEQ SET starttime = CURRENT_TIMESTAMP(), status = 'Archive' WHERE program_seq_pk = '{0}'", pk);
            //Running 으로 변경
            cmd = new MySqlCommand(m_sql, connPool.getConnection());
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

        public String GetYoutubeSessionID(String cid)
        {
            DataSet ds = new DataSet();
            String session_id;
            connPool.ConnectionOpen();
            m_sql = String.Format("SELECT session_id FROM TB_YOUTUBE WHERE cid = '{0}'", cid);
            MySqlDataAdapter adpt = new MySqlDataAdapter(m_sql, connPool.getConnection());
            adpt.Fill(ds, "GET_SID");
            connPool.ConnectionClose();

            session_id = ds.Tables[0].Rows[0]["session_id"].ToString();
            
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
                thumbnail_img = "";
            }

            return thumbnail_img;
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

        public bool UpdateFtpCompleted(String pk)
        {
            m_sql = String.Format("UPDATE TB_FTP_QUEUE SET endtime = CURRENT_TIMESTAMP(), status = 'Completed', percent = 100 WHERE ftp_queue_pk = '{0}'", pk);

            connPool.ConnectionOpen();
            cmd = new MySqlCommand(m_sql, connPool.getConnection());
            cmd.ExecuteNonQuery();
            connPool.ConnectionClose();
            return true;
        }

        public bool UpdateFtpFailed(String pk)
        {
            m_sql = String.Format("UPDATE TB_FTP_QUEUE SET endtime = CURRENT_TIMESTAMP(), status = 'Failed', percent = 100 WHERE ftp_queue_pk = '{0}'", pk);

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

        public bool UpdateProgramImgCompleted(String full_url, String pid)
        {
            connPool.ConnectionOpen();
            m_sql = String.Format(@"UPDATE TB_PROGRAM
                                    SET cdnurl_img = '{0}', status = 'Completed'
                                    WHERE pid = '{1}'", full_url, pid);
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

        public bool UpdateSetWaitingCallBack(String pk)
        {
            connPool.ConnectionOpen();
            m_sql = String.Format(@"UPDATE TB_CLIP SET status = 'Waiting Callback' WHERE clip_pk = '{0}'", pk);
            cmd = new MySqlCommand(m_sql, connPool.getConnection());
            cmd.ExecuteNonQuery();
            connPool.ConnectionClose();
            return true;
        }

        public bool UpdateProgramSEQReady(String gid)
        {
            connPool.ConnectionOpen();
            m_sql = String.Format(@"UPDATE TB_PROGRAM_SEQ
                                    SET status = 'Ready'
                                    WHERE gid = '{0}'", gid);
            cmd = new MySqlCommand(m_sql, connPool.getConnection());
            cmd.ExecuteNonQuery();
            connPool.ConnectionClose();
            return true;
        }

        public bool UpdateClipReady(String pk)
        {
            connPool.ConnectionOpen();
            m_sql = String.Format(@"UPDATE TB_CLIP
                                    SET status = 'Ready'
                                    WHERE clip_pk = '{0}'", pk);
            cmd = new MySqlCommand(m_sql, connPool.getConnection());
            cmd.ExecuteNonQuery();
            connPool.ConnectionClose();
            return true;
        }

        public bool UpdateProgramSEQCompleted(String gid)
        {
            connPool.ConnectionOpen();
            m_sql = String.Format(@"UPDATE TB_PROGRAM_SEQ
                                    SET status = 'Completed'
                                    WHERE gid = '{0}'", gid);
            cmd = new MySqlCommand(m_sql, connPool.getConnection());
            cmd.ExecuteNonQuery();
            connPool.ConnectionClose();
            return true;
        }

        public bool UpdateClipCompleted(String pk)
        {
            connPool.ConnectionOpen();
            m_sql = String.Format(@"UPDATE TB_CLIP
                                    SET status = 'Completed'
                                    WHERE clip_pk = '{0}'", pk);
            cmd = new MySqlCommand(m_sql, connPool.getConnection());
            cmd.ExecuteNonQuery();
            connPool.ConnectionClose();
            return true;
        }

        public bool UpdateClipStatus(String cid, String status)
        {
            connPool.ConnectionOpen();
            m_sql = String.Format(@"UPDATE TB_CLIP
                                    SET status = '{1}'
                                    WHERE cid = '{0}'", cid, status);
            cmd = new MySqlCommand(m_sql, connPool.getConnection());
            cmd.ExecuteNonQuery();
            connPool.ConnectionClose();
            return true;
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

        public bool UpdateDailymotionStatus(String cid, String status)
        {
            connPool.ConnectionOpen();
            m_sql = String.Format(@"UPDATE TB_DAILYMOTION
                                    SET status = '{0}'                                
                                    WHERE cid = '{1}'", status, cid);
            cmd = new MySqlCommand(m_sql, connPool.getConnection());
            cmd.ExecuteNonQuery();
            connPool.ConnectionClose();
            return true;
        }

        public bool UpdateClipCompletedBycid(String cid)
        {
            connPool.ConnectionOpen();
            m_sql = String.Format(@"UPDATE TB_CLIP
                                    SET status = 'Completed'
                                    WHERE cid = '{0}'", cid);
            cmd = new MySqlCommand(m_sql, connPool.getConnection());
            cmd.ExecuteNonQuery();
            connPool.ConnectionClose();
            return true;
        }

        public bool UpdateCDNURL(String type, String full_url, String pk)
        {
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