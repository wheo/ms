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
    class FTPService
    {
        private Boolean _shouldStop = false;
        // put this className
        private Log log;
        private Object lockObject = new Object();

        public FTPService()
        {
            DoWork();            
        }

        public FTPService(int sleeptime)
        {
            Thread.Sleep(sleeptime);
            DoWork();
        }
        
        void DoWork()
        {
            log = new Log(this.GetType().Name);
            Thread t1 = new Thread(() => Run("normal"));
            Thread t2 = new Thread(() => Run("normal"));
            Thread t3 = new Thread(() => Run("yt"));
            Thread t4 = new Thread(() => Run("dm"));
            
            t1.Start();
            Thread.Sleep(1000);
            t2.Start();
            Thread.Sleep(1000);
            t3.Start();
            Thread.Sleep(1000);
            t4.Start();
        }

        public void RequestStop()
        {
            _shouldStop = true;
        }
        private void GetFullPathUrl(CdnInfo cdninfo, String type, out String full_url, out String rtmp_url, out String hls_url, String target_path, String target_filename)
        {
            full_url = "";
            rtmp_url = "";
            hls_url = "";
            String accessCheck = "";
            String strFTPid = "";            
            JObject obj;

            if (type.ToLower().Equals("mov"))
            {
                strFTPid = "mbcplus_mbcplus-dn"; // mov
            }
            else if (type.ToLower().Equals("img"))
            {
                strFTPid = "mbcplus_mbcplus-img"; // img
            }
            else if (type.ToLower().Equals("srt"))
            {
                strFTPid = "mbcplus_mbcplus-img"; // srt
            } else
            {
                strFTPid = "mbcplus_mbcplus-img"; // yt_img, yt_srt 포함
            }
            
            // apiFtpPath : 상대주소 mbcplus/...../파일이름720p2M.mp4
            String apiFtpPath = String.Format(@"{0}/{1}", target_path, target_filename);
            String pathfilename = apiFtpPath;

            JObject metaJson = new JObject(
            new JProperty("api_request",
            new JObject(
                new JProperty("ftpid", strFTPid),
                new JProperty("key", cdninfo.strAPIKey),
                new JProperty("file_path", pathfilename))));

            String strJson = "";
            // strCDNMethod : 0 is downpath
            String uri = cdninfo.strCDNHost + cdninfo.strCDNMethod[0];
            strJson = metaJson.ToString();

            log.logging("url : " + uri);
            log.logging("strJson : " + strJson);
            var response = Http.Post(uri, strJson);
            string responseString = Encoding.UTF8.GetString(response);
            log.logging("response : " + responseString);

            try
            {
                obj = JObject.Parse(responseString);
                accessCheck = (String)obj["api_response"]["access_test_result"];
                full_url = (String)obj["api_response"]["full_url"];
            }
            catch (Exception e)
            {
                log.logging("Parse Error : " + e.ToString());
            }
            
            log.logging("accessCheck : " + accessCheck);
            log.logging("full_url : " + full_url);            

            //mbcplus_mbcplus-img / mbcplus1@
            strFTPid = "mbcplus_mbcpvod";

            metaJson = new JObject(
                new JProperty("api_request",
                    new JObject(
                        new JProperty("ftpid", strFTPid),
                        new JProperty("key", cdninfo.strAPIKey),
                        new JProperty("file_path", pathfilename),
                        new JProperty("file_type", "mp4"),
                        new JProperty("stream_type", "rtmp"))));
            uri = cdninfo.strCDNHost + cdninfo.strCDNMethod[1];
            strJson = metaJson.ToString();

            log.logging("url : " + uri);
            log.logging("strJson : " + strJson);

            response = Http.Post(uri, strJson);
            responseString = Encoding.UTF8.GetString(response);

            log.logging("response : " + responseString);
            try
            {
                obj = JObject.Parse(responseString);
                accessCheck = (String)obj["api_response"]["access_test_result"];
                rtmp_url = (String)obj["api_response"]["full_url"];
            }
            catch (Exception e)
            {
                log.logging("Parse Error : " + e.ToString());
            }

            metaJson = new JObject(
                new JProperty("api_request",
                    new JObject(
                        new JProperty("ftpid", strFTPid),
                        new JProperty("key", cdninfo.strAPIKey),
                        new JProperty("file_path", pathfilename),
                        new JProperty("file_type", "mp4"),
                        new JProperty("stream_type", "hls"))));
            uri = cdninfo.strCDNHost + cdninfo.strCDNMethod[1];
            strJson = metaJson.ToString();

            log.logging(uri);
            log.logging(strJson);
            response = Http.Post(uri, strJson);
            responseString = Encoding.UTF8.GetString(response);
            log.logging(responseString);

            obj = JObject.Parse(responseString);
            accessCheck = (String)obj["api_response"]["access_test_result"];
            hls_url = (String)obj["api_response"]["full_url"];
        }

        void Run(String customer_type)
        {
            //YTInfo ytInfo = Singleton.getInstance().Get_YTInstance();           

            ConnectionPool connPool;
            SqlMapper mapper = new SqlMapper();            
            //String type = null;
            //MySqlCommand cmd;
            connPool = new ConnectionPool();
            connPool.SetConnection(new MySqlConnection(Singleton.getInstance().GetStrConn()));            

            /* Properties.ini로 옮김 ( 주석처리 된 곳 삭제 예정)
            String strBaseUri = "http://metaapi.mbcmedia.net:5000/SMRMetaCollect.svc/"; // MetaHubApi Agent URL (삭제예정)
            String strBBMCHost = "http://mbcplus.mediacast.co.kr/";
            // 2017-12-13 변경됨 // softcoding으로 바꿀 것
            strBBMCHost = "http://mbcplus2.mediacast.co.kr";
            */
            String strBBMCHost = Singleton.getInstance().BBMChost;

            CdnInfo cdninfo = Singleton.getInstance().GetCdnInfo();
            String apiFtpPath = "";

            /* // 2018-01-04 deprecated
            String metahubQuery = "";
            if (m_isMetahub)
            {
                metahubQuery = "AND TB_CUSTOMER.customer_id = '1'";
            }
            else
            {
                metahubQuery = "AND TB_CUSTOMER.customer_id != '1'";
            }
             */

            String sql_tail = "";
            if (customer_type == "yt") // if customer_
            {
                sql_tail = "AND CU.customer_id = '9'";
            }
            else if (customer_type == "dm")
            {
                sql_tail = "AND CU.customer_id = '10'";
            }
            else
            {
                sql_tail = "AND CU.customer_id != '9' AND CU.customer_id != '10'";
            }
            //Waiting for make winform
            Thread.Sleep(5000);            
            while (!_shouldStop)
            {                
                try
                {
                    DataSet ds = new DataSet();
                    String[] callbackurl;
                    callbackurl = Singleton.getInstance().GetStrCalalbackURL();
                    vo.FtpInfo ftpInfo = new vo.FtpInfo();
                    YTInfo ytInfo = new YTInfo();
                    DMInfo dmInfo = new DMInfo();
                    String tc_pk = null;
                    lock (lockObject)
                    {
                        mapper.GetFtpInfo(ds, sql_tail);
                    }
                    
                    // Rows 가 1 이상일 때 탐색함
                    foreach (DataRow r in ds.Tables[0].Rows)
                    {
                        try
                        {
                            ftpInfo.pk = r["pk"].ToString();
                            ftpInfo.clip_pk = r["clip_pk"].ToString();
                            ftpInfo.customer_id = r["customer_id"].ToString();
                            ftpInfo.srcpath = r["srcpath"].ToString();
                            ftpInfo.targetpath = r["targetpath"].ToString();
                            ftpInfo.targetfilename = r["targetfilename"].ToString();
                            //old로 기존 파일이름이름 남김
                            ftpInfo.old_targetfilename = ftpInfo.targetfilename;
                            ftpInfo.attribute = r["attribute"].ToString();
                            ftpInfo.customer_name = r["customer_name"].ToString();
                            ftpInfo.transcoding_YN = r["transcoding_YN"].ToString();
                            ftpInfo.alias_YN = r["alias_YN"].ToString();
                            ftpInfo.s_title = r["s_title"].ToString();
                            ftpInfo.title = r["title"].ToString();
                            ftpInfo.broaddate = r["broaddate"].ToString();
                            ftpInfo.sportskind = r["sportskind"].ToString();
                            ftpInfo.team1 = r["team1"].ToString();
                            ftpInfo.team2 = r["team2"].ToString();
                            ftpInfo.inning = r["inning"].ToString();
                            ftpInfo.host = r["host"].ToString();
                            ftpInfo.port = r["port"].ToString();
                            ftpInfo.path = r["path"].ToString();
                            ftpInfo.id = r["id"].ToString();
                            ftpInfo.pw = r["pw"].ToString();
                            ftpInfo.status = r["status"].ToString();
                            ftpInfo.type = r["type"].ToString();
                            ftpInfo.clip_YN = r["clip_YN"].ToString();
                            ftpInfo.pid = r["fq_pid"].ToString();
                            ftpInfo.gid = r["gid"].ToString();
                            ftpInfo.cid = r["cid"].ToString();
                            ftpInfo.program_img_type = r["program_img_type"].ToString();
                            ftpInfo.smr_pid = r["smr_pid"].ToString();
                            ftpInfo.smr_img_type = r["smr_img_type"].ToString();
                            ftpInfo.metahub_YN = r["metahub_YN"].ToString();
                            ftpInfo.s_metahub_YN = r["s_metahub_YN"].ToString();
                            ftpInfo.cdn_img = r["cdnurl_img"].ToString();
                            ftpInfo.cdn_mov = r["cdnurl_mov"].ToString();
                            ftpInfo.s_ip4addr = callbackurl[0]; // NAT 첫번째 값으로 고정 2017-12-13
                            ftpInfo.clip_img_edit_count = Convert.ToInt32(r["clip_img_edit_count"].ToString());
                            ftpInfo.clip_mov_edit_count = Convert.ToInt32(r["clip_mov_edit_count"].ToString());

                            ftpInfo.program_img_edit_count = Convert.ToInt32(r["program_img_edit_count"].ToString());
                            ftpInfo.program_posterimg_edit_count = Convert.ToInt32(r["program_posterimg_edit_count"].ToString());
                            ftpInfo.program_circleimg_edit_count = Convert.ToInt32(r["program_circleimg_edit_count"].ToString());
                            ftpInfo.program_thumbimg_edit_count = Convert.ToInt32(r["program_thumbimg_edit_count"].ToString());                            

                            ftpInfo.smr_program_img_edit_count = Convert.ToInt32(r["smr_program_img_edit_count"].ToString());
                            ftpInfo.smr_program_posterimg1_edit_count = Convert.ToInt32(r["smr_program_posterimg1_edit_count"].ToString());
                            ftpInfo.smr_program_posterimg2_edit_count = Convert.ToInt32(r["smr_program_posterimg2_edit_count"].ToString());
                            ftpInfo.smr_program_bannerimg_edit_count = Convert.ToInt32(r["smr_program_bannerimg_edit_count"].ToString());
                            ftpInfo.smr_program_thumbimg_edit_count = Convert.ToInt32(r["smr_program_thumbimg_edit_count"].ToString());

                            ftpInfo.program_seq_img_edit_count = Convert.ToInt32(r["program_seq_img_edit_count"].ToString());
                            ftpInfo.youtube_img_edit_count = Convert.ToInt32(r["youtube_img_edit_count"].ToString());
                            ftpInfo.dailymotion_img_edit_count = Convert.ToInt32(r["dailymotion_img_edit_count"].ToString());

                            ytInfo.cid = ftpInfo.cid;
                            ytInfo.videoid = r["yt_videoid"].ToString();
                            ytInfo.channelid = r["yt_channel_id"].ToString();
                            ytInfo.playlist_id = r["yt_playlist_id"].ToString();
                            // youtube playlist 형식으로 변환
                            ytInfo.playlist_id = ytInfo.playlist_id.Replace(",", "|");
                            ytInfo.title = r["yt_title"].ToString();
                            ytInfo.description = r["yt_description"].ToString();
                            ytInfo.tags = r["yt_tag"].ToString().Replace(",", "|");
                            ytInfo.isuse = r["yt_isuse"].ToString();
                            ytInfo.category = r["yt_category"].ToString();
                            ytInfo.enable_contentid = r["yt_enable_contentid"].ToString();
                            ytInfo.uploader_name = r["yt_uploader_name"].ToString();
                            ytInfo.upload_control_id = r["yt_upload_control_id"].ToString();
                            ytInfo.usage_policy = r["yt_usage_policy"].ToString();
                            ytInfo.match_policy = r["yt_match_policy"].ToString();
                            ytInfo.start_time = r["yt_start_time"].ToString();
                            ytInfo.spoken_language = r["yt_spoken_language"].ToString();
                            ytInfo.target_language = r["yt_target_language"].ToString();
                            ytInfo.custom_id = r["yt_custom_id"].ToString();
                            ytInfo.information = r["yt_information"].ToString();
                            ytInfo.sh_custom_id = r["pid"].ToString();
                            ytInfo.ep_original_release_date = r["ep_original_release_date"].ToString();
                            ytInfo.ep_number = r["s_contentnumber"].ToString();
                            ytInfo.status = r["yt_status"].ToString();
                            ytInfo.ownership = r["yt_ownership"].ToString();

                            dmInfo.cid = ftpInfo.cid;
                            dmInfo.videoid = r["dm_videoid"].ToString();
                            dmInfo.title = r["dm_title"].ToString();
                            dmInfo.description = r["dm_description"].ToString();
                            dmInfo.category = r["dm_category"].ToString();
                            dmInfo.publish_date = r["dm_publish_date"].ToString();
                            dmInfo.expiry_date = r["dm_expiry_date"].ToString();
                            dmInfo.url = r["dm_url"].ToString();
                            dmInfo.tags = r["dm_tag"].ToString();
                            dmInfo.isuse = r["dm_isuse"].ToString();
                            dmInfo.status = r["dm_status"].ToString();
                            dmInfo.playlistid = r["dm_playlistid"].ToString();
                            dmInfo.geoblock_code = r["geoblock_code"].ToString();
                            dmInfo.geoblock_value = r["geoblock_value"].ToString();

                            //frmMain.WriteLogThread("[FTPService] CallbackURL : " + m_s_ip4addr);
                            
                            // 해피라이징의 경우 clip_YN 이 N으로 되어있으며 MOV 의 경우 트랜스코딩 후 파일은 보내지 않고 XML로 경로만 보냄
                            if (String.Equals("N", ftpInfo.clip_YN) && String.Equals("MOV", ftpInfo.type))
                            {
                                mapper.UpdateContentBypass(ftpInfo.pk);
                                //frmMain.WriteLogThread(String.Format(@"[FTPService] {0} clip is bypassed", m_clip_pk));
                                continue;
                            }

                            String edit_count_title = null;

                            // 파일이름을 변경해서 보내야할 고객사의 경우는 아래의 로직을 따름
                            if (ftpInfo.alias_YN == "Y")
                            {
                                // 검증해야함 // 클립 수정없는 재전송 로직도 추가해야함
                                String alias_targetpath = "";
                                String alias_targetfilename = "";
                                String redistribute_title = null;
                                

                                if (ftpInfo.clip_mov_edit_count > 1 )
                                {
                                    redistribute_title = "_재전송";
                                }
                                if (ftpInfo.clip_mov_edit_count > 1)
                                {
                                    edit_count_title = String.Format("_수정_{0}", ftpInfo.clip_mov_edit_count - 1);
                                }

                                alias_targetpath = String.Format(@"{0}/{1}", ftpInfo.broaddate, Util.escapedPath(Util.repaceInvalidFilename(ftpInfo.s_title)));
                                alias_targetfilename = String.Format(@"{0}_{1}_vs_{2}_{3}_{4}{5}{6}{7}", ftpInfo.broaddate
                                    , ftpInfo.team1
                                    , ftpInfo.team2
                                    , ftpInfo.inning
                                    , Util.escapedPath(Util.repaceInvalidFilename(ftpInfo.title))
                                    , Path.GetExtension(Util.repaceInvalidFilename(ftpInfo.targetfilename))
                                    , redistribute_title
                                    , edit_count_title);

                                log.logging(String.Format("{0} alias is {1}", ftpInfo.cid, alias_targetfilename));

                                mapper.UpdateAliasFilepath(ftpInfo.pk, alias_targetpath, alias_targetfilename);

                                ftpInfo.targetpath = alias_targetpath.Replace("''", "'");
                                ftpInfo.targetfilename = alias_targetfilename.Replace("''", "'");
                            }

                            /*
                            if (ftpInfo.clip_mov_edit_count > 1 && ftpInfo.type.ToLower() == "mov")
                            {
                                edit_count_title = String.Format("_{0}", (ftpInfo.clip_mov_edit_count - 1).ToString("D2"));
                                ftpInfo.targetfilename = String.Format("{0}{1}{2}", Path.GetFileNameWithoutExtension(ftpInfo.targetfilename), edit_count_title, Path.GetExtension(ftpInfo.targetfilename));
                            }
                            if (ftpInfo.clip_img_edit_count > 1 && ftpInfo.type.ToLower() == "img")
                            {
                                edit_count_title = String.Format("_{0}", (ftpInfo.clip_img_edit_count - 1).ToString("D2"));
                                ftpInfo.targetfilename = String.Format("{0}{1}{2}", Path.GetFileNameWithoutExtension(ftpInfo.targetfilename), edit_count_title, Path.GetExtension(ftpInfo.targetfilename));
                            }

                            if ( ftpInfo.program_img_edit_count > 1 && ftpInfo.type.ToLower() == "img" && ftpInfo.program_img_type == "1")
                            {
                                edit_count_title = String.Format("_{0}", (ftpInfo.program_img_edit_count - 1).ToString("D2"));
                                ftpInfo.targetfilename = String.Format("{0}{1}{2}", Path.GetFileNameWithoutExtension(ftpInfo.targetfilename), edit_count_title, Path.GetExtension(ftpInfo.targetfilename));
                            }
                            if (ftpInfo.program_posterimg_edit_count > 1 && ftpInfo.type.ToLower() == "img" && ftpInfo.program_img_type == "2")
                            {
                                edit_count_title = String.Format("_{0}", (ftpInfo.program_posterimg_edit_count - 1).ToString("D2"));
                                ftpInfo.targetfilename = String.Format("{0}{1}{2}", Path.GetFileNameWithoutExtension(ftpInfo.targetfilename), edit_count_title, Path.GetExtension(ftpInfo.targetfilename));
                            }
                            if (ftpInfo.program_thumbimg_edit_count > 1 && ftpInfo.type.ToLower() == "img" && ftpInfo.program_img_type == "3")
                            {
                                edit_count_title = String.Format("_{0}", (ftpInfo.program_thumbimg_edit_count - 1).ToString("D2"));
                                ftpInfo.targetfilename = String.Format("{0}{1}{2}", Path.GetFileNameWithoutExtension(ftpInfo.targetfilename), edit_count_title, Path.GetExtension(ftpInfo.targetfilename));
                            }
                            if (ftpInfo.program_circleimg_edit_count > 1 && ftpInfo.type.ToLower() == "img" && ftpInfo.program_img_type == "4")
                            {
                                edit_count_title = String.Format("_{0}", (ftpInfo.program_circleimg_edit_count - 1).ToString("D2"));
                                ftpInfo.targetfilename = String.Format("{0}{1}{2}", Path.GetFileNameWithoutExtension(ftpInfo.targetfilename), edit_count_title, Path.GetExtension(ftpInfo.targetfilename));
                            }
                            if (String.IsNullOrEmpty(ftpInfo.cid))
                            {
                                if (ftpInfo.program_seq_img_edit_count > 1 && ftpInfo.type.ToLower() == "img")
                                {
                                    edit_count_title = String.Format("_{0}", (ftpInfo.program_seq_img_edit_count - 1).ToString("D2"));
                                    ftpInfo.targetfilename = String.Format("{0}{1}{2}", Path.GetFileNameWithoutExtension(ftpInfo.targetfilename), edit_count_title, Path.GetExtension(ftpInfo.targetfilename));
                                }
                            }

                            if ( ftpInfo.smr_program_img_edit_count > 1 && ftpInfo.type.ToLower() == "img" && ftpInfo.smr_img_type == "1")
                            {
                                edit_count_title = String.Format("_{0}", (ftpInfo.smr_program_img_edit_count - 1).ToString("D2"));
                                ftpInfo.targetfilename = String.Format("{0}{1}{2}", Path.GetFileNameWithoutExtension(ftpInfo.targetfilename), edit_count_title, Path.GetExtension(ftpInfo.targetfilename));
                            }
                            if (ftpInfo.smr_program_posterimg1_edit_count > 1 && ftpInfo.type.ToLower() == "img" && ftpInfo.smr_img_type == "2")
                            {
                                edit_count_title = String.Format("_{0}", (ftpInfo.smr_program_posterimg1_edit_count - 1).ToString("D2"));
                                ftpInfo.targetfilename = String.Format("{0}{1}{2}", Path.GetFileNameWithoutExtension(ftpInfo.targetfilename), edit_count_title, Path.GetExtension(ftpInfo.targetfilename));
                            }
                            if (ftpInfo.smr_program_posterimg2_edit_count > 1 && ftpInfo.type.ToLower() == "img" && ftpInfo.smr_img_type == "3")
                            {
                                edit_count_title = String.Format("_{0}", (ftpInfo.smr_program_posterimg2_edit_count - 1).ToString("D2"));
                                ftpInfo.targetfilename = String.Format("{0}{1}{2}", Path.GetFileNameWithoutExtension(ftpInfo.targetfilename), edit_count_title, Path.GetExtension(ftpInfo.targetfilename));
                            }
                            if (ftpInfo.smr_program_bannerimg_edit_count > 1 && ftpInfo.type.ToLower() == "img" && ftpInfo.smr_img_type == "4")
                            {
                                edit_count_title = String.Format("_{0}", (ftpInfo.smr_program_bannerimg_edit_count - 1).ToString("D2"));
                                ftpInfo.targetfilename = String.Format("{0}{1}{2}", Path.GetFileNameWithoutExtension(ftpInfo.targetfilename), edit_count_title, Path.GetExtension(ftpInfo.targetfilename));
                            }
                            if (ftpInfo.smr_program_thumbimg_edit_count > 1 && ftpInfo.type.ToLower() == "img" && ftpInfo.smr_img_type == "5")
                            {
                                edit_count_title = String.Format("_{0}", (ftpInfo.smr_program_thumbimg_edit_count - 1).ToString("D2"));
                                ftpInfo.targetfilename = String.Format("{0}{1}{2}", Path.GetFileNameWithoutExtension(ftpInfo.targetfilename), edit_count_title, Path.GetExtension(ftpInfo.targetfilename));
                            }

                            if ( ftpInfo.youtube_img_edit_count > 1 && ftpInfo.type.ToLower() == "img")
                            {
                                edit_count_title = String.Format("_{0}", (ftpInfo.youtube_img_edit_count - 1).ToString("D2"));
                                ftpInfo.targetfilename = String.Format("{0}{1}{2}", Path.GetFileNameWithoutExtension(ftpInfo.targetfilename), edit_count_title, Path.GetExtension(ftpInfo.targetfilename));
                            }
                            if (ftpInfo.dailymotion_img_edit_count > 1 && ftpInfo.type.ToLower() == "img")
                            {
                                edit_count_title = String.Format("_{0}", (ftpInfo.dailymotion_img_edit_count - 1).ToString("D2"));
                                ftpInfo.targetfilename = String.Format("{0}{1}{2}", Path.GetFileNameWithoutExtension(ftpInfo.targetfilename), edit_count_title, Path.GetExtension(ftpInfo.targetfilename));
                            }
                            */

                            // 파일 타겟 네임 설정 부분
                            FTPMgr ftpmgr = new FTPMgr(connPool);
                            ftpmgr.SetAlias(ftpInfo.alias_YN);
                            ftpmgr.SetConfidential(ftpInfo.pk, ftpInfo.host, ftpInfo.path, ftpInfo.id, ftpInfo.pw, ftpInfo.port, ftpInfo.customer_id, ftpInfo.gid, ftpInfo.cid);
                            ftpmgr.SetSourcePath(ftpInfo.srcpath);
                            ftpmgr.SetTargetPath(ftpInfo.targetpath + "/" + ftpInfo.targetfilename);

                            Dictionary<String, Object> map = new Dictionary<String, Object>();

                            map = r.Table.Columns
                                .Cast<DataColumn>()
                                .ToDictionary(col => col.ColumnName, col => r.Field<Object>(col.ColumnName));

                            if (ftpInfo.type.ToLower() == "xml")
                            {
                                // 전송해야할 대상이 XML이라면 이 시점에서 XML 생성
                                // HLS, RTMP, download url 이 빠진 xml을 생성
                                MBCPlusMeta mbcplusMeta = new MBCPlusMeta();
                                mbcplusMeta.MakeXML(map);

                                XmlDocument xmlDoc = mbcplusMeta.GetCurrentXmlDocument();
                                xmlDoc.Save(ftpInfo.srcpath);
                            }
                            String errmsg = "";
                            // youtube upload
                            if (ftpInfo.customer_id == "9")
                            {
                                // 채널 리스트 갱신
                                // 플레이 리스트 갱신
                                // 저작권 차단 목록 갱신
                                
                                String session_id = mapper.GetYoutubeSessionID(ftpInfo.cid);
                                if ( String.IsNullOrEmpty(session_id) )
                                {
                                    ytInfo.session_id = DateTime.Now.Ticks.ToString("x");
                                    mapper.UpdateYoutubeSessionID(ftpInfo.cid, ytInfo.session_id);
                                }
                                else
                                {
                                    ytInfo.session_id = session_id;
                                }

                                if (ftpInfo.type.ToLower() == "xml")
                                {
                                    mapper.UpdateContentBypass(ftpInfo.pk);
                                }
                                else if (ftpmgr.SendFile("yt", ftpInfo.type, ytInfo))
                                {
                                    // 이 시점에서 SRT, IMG, MPG 전부 전송됨(순서는 아직 지정되지 않음)
                                    frmMain.WriteLogThread(String.Format(@"[FTPService] (Youtube) {0} is completed", ftpInfo.cid));
                                    mapper.UpdateYoutubeStatus(ftpInfo.cid, "Waiting videoid");
                                    log.logging(String.Format("{0} ({1}) : {2}", ftpInfo.cid, ftpInfo.type, "Waiting videoid"));
                                    //make csvfile
                                    if (ytInfo.IsMovCompleted)
                                    {
                                        frmMain.WriteLogThread("send yt_videoid check : " + ytInfo.videoid);
                                        DataSet ds_account = new DataSet();
                                        if (!String.IsNullOrEmpty(ytInfo.videoid))
                                        {
                                            frmMain.WriteLogThread("delete yt_videoid  : " + ytInfo.videoid);
                                            mapper.GetYTAccountList(ds_account,ytInfo.channelid);
                                            ytInfo.authentication(ds_account.Tables[0].Rows[0]["keyfile"].ToString(), ds_account.Tables[0].Rows[0]["name"].ToString());
                                            try
                                            {
                                                ytInfo.DeleteVideo(ytInfo.videoid);
                                            } catch { }
                                            
                                            //WMS videoid 삭제
                                            mapper.UpdateYoutubeVideoIDToNULL(ytInfo.videoid);
                                            frmMain.WriteLogThread("delete yt_videoid  : " + ytInfo.videoid + " is Completed");
                                        }
                                        String thumb_FileName = null;
                                        String SRT_FileName = null;
                                        //ytInfo.IsImgCompleted = mapper.YTimgUploadCheck(cid, out thumb_Filename);
                                        ytInfo.IsImgCompleted = mapper.YTUploadCheck(ftpInfo.cid, "img", out thumb_FileName);
                                        ytInfo.IsSrtCompleted = mapper.YTUploadCheck(ftpInfo.cid, "srt", out SRT_FileName);

                                        if ( !String.IsNullOrEmpty(thumb_FileName) )
                                        {
                                            ytInfo.custom_thumbnail = thumb_FileName;
                                        }
                                        if ( !String.IsNullOrEmpty(SRT_FileName))
                                        {
                                            ytInfo.caption_file = SRT_FileName;
                                            ytInfo.caption_language = ytInfo.spoken_language;
                                        }
                                        String csvFileName = ytInfo.MakeYoutubeCSVFile();
                                        
                                        String csv_destPath = String.Format("/{0}_{1}/{2}", Path.GetFileNameWithoutExtension(ytInfo.movpath), ytInfo.session_id, Path.GetFileName(csvFileName));
                                        
                                        frmMain.WriteLogThread(String.Format("{0} is created", csvFileName));
                                        ftpmgr.yt_csvSendFile(csvFileName, csv_destPath);
                                        ftpmgr.yt_deliveryCompleteSendFile(csv_destPath);
                                        //2019-03-14 추가
                                        mapper.UpdateClipStatus(ftpInfo.cid, "Completed");
                                        //Youtube service ready to parse                                        
                                        mapper.UpdateYoutubeReady(ftpInfo.cid);
                                    }
                                    mapper.UpdateFtpCompleted(ftpInfo.pk);

                                    //thumbnail과 mov의 업로드가 되었는지 체크
                                    //Delevery.Completed 전송함
                                    //status xml 생성을 기다릴 수 있게 YoutubeService로 넘김
                                }
                                else
                                {
                                    // 전송 실패
                                    mapper.UpdateFtpStatus(ftpInfo.pk, "Failed");
                                    mapper.UpdateClipStatus(ftpInfo.cid, "Failed");                                                                  
                                }
                            }
                            // dailymotion upload
                            else if (ftpInfo.customer_id == "10")
                            {
                                if (ftpInfo.type.ToLower() == "img" | ftpInfo.type.ToLower() == "xml" | ftpInfo.type.ToLower() == "srt")
                                {
                                    mapper.UpdateContentBypass(ftpInfo.pk);
                                }
                                else
                                {
                                    if (ftpmgr.SendDailymotion(ftpInfo.type, dmInfo))
                                    {
                                        //업로드 후 만약 비디오 아이디가 존재 하면 컨텐츠 삭제 후 videoid 공백으로 변경
                                        if (!String.IsNullOrEmpty(dmInfo.videoid))
                                        {
                                            //기존 비디오 삭제
                                            String response = null;
                                            dmInfo.DeleteVideo(dmInfo.videoid, out response);
                                            frmMain.WriteLogThread(String.Format("Dailymotion videoid({0} is deleted)", dmInfo.videoid));
                                            log.logging(String.Format("Dailymotion videoid({0} is deleted) response : {1}", dmInfo.videoid, response));
                                        }
                                        mapper.UpdateFtpCompleted(ftpInfo.pk);
                                        frmMain.WriteLogThread(String.Format(@"[FTPService] (Dailymotion) {0} is completed", ftpInfo.pk));

                                        mapper.UpdateDailyMotionStatus(ftpInfo.cid, "Completed");
                                        mapper.UpdateClipStatus(ftpInfo.cid, "Completed");
                                    }
                                    else
                                    {
                                        mapper.UpdateFtpStatus(ftpInfo.pk, "Failed");
                                        //mapper.UpdateFtpFailed(pk);
                                        mapper.UpdateClipStatus(ftpInfo.cid, "Failed");
                                        frmMain.WriteLogThread(String.Format(@"[FTPService] (Dailymotion) {0} is Failed", ftpInfo.pk));
                                    }
                                }
                            }
                            else if (ftpmgr.SendFile(out errmsg))
                            {
                                /*
                                // 과거 파일이 있으면 삭제해야함 // 삭제 보류
                                if (ftpmgr.Legacycheck(ftpInfo))
                                {
                                    ftpmgr.DeleteFile();

                                } else
                                {
                                    //false
                                }
                                */

                                // 파일전송이 완료 되어 상태를 Completed 으로 변경
                                mapper.UpdateFtpCompleted(ftpInfo.pk);
                                frmMain.WriteLogThread(String.Format(@"[FTPService] {0} is completed", ftpInfo.pk));

                                String strRequest = "";
                                String strResponse = "";

                                // customer_id : 2 LG_CDN 일 경우 full_path를 요청함
                                if (ftpInfo.customer_id == "2")
                                {
                                    String full_url = "";
                                    String rtmp_url = "";
                                    String hls_url = "";

                                    GetFullPathUrl(cdninfo, ftpInfo.type, out full_url, out rtmp_url, out hls_url, ftpInfo.targetpath, ftpInfo.targetfilename);

                                    String apiPurgeDomain;
                                    String cdnapi;
                                    String cdnresponse;
                                    apiFtpPath = ftpInfo.targetpath + "/" + ftpInfo.targetfilename;

                                    if (!String.IsNullOrEmpty(ftpInfo.clip_pk) && ftpInfo.type.ToLower().Equals("mov"))
                                    {
                                        //클립 영상
                                        // 전송한 파일이 clip mov며 CDN일 때 customer_id : 2가 CDN
                                        // 1. 영상 원본 퍼지
                                        // 2. full_url 업데이트                                        

                                        apiPurgeDomain = "http://vod.mbcmpp.co.kr";
                                        cdnapi = cdninfo.apiDomain + "?user_id=" + cdninfo.apiUserid + "&passwd=" + cdninfo.apiPasswd + "&action=" + cdninfo.apiAction + "&purge_domain=" + apiPurgeDomain + "&purge_url=" + apiFtpPath;
                                        log.logging(cdnapi);
                                        cdnresponse = Http.Get(cdnapi);
                                        log.logging(cdnresponse);

                                        apiPurgeDomain = "http://mov.mbcmpp.co.kr";
                                        cdnapi = cdninfo.apiDomain + "?user_id=" + cdninfo.apiUserid + "&passwd=" + cdninfo.apiPasswd + "&action=" + cdninfo.apiAction + "&purge_domain=" + apiPurgeDomain + "&purge_url=" + apiFtpPath;
                                        log.logging("cdnapi : " + cdnapi);
                                        cdnresponse = Http.Get(cdnapi);
                                        log.logging(cdnresponse);
                                        mapper.UpdateClipInfos(full_url, rtmp_url, hls_url, ftpInfo.clip_pk);
                                    }
                                    if (!String.IsNullOrEmpty(ftpInfo.clip_pk) && ftpInfo.type.ToLower().Equals("srt"))
                                    {
                                        //자막파일 SRT
                                        apiPurgeDomain = "http://Mov.mbcmpp.co.kr";
                                        cdnapi = cdninfo.apiDomain + "?user_id=" + cdninfo.apiUserid + "&passwd=" + cdninfo.apiPasswd + "&action=" + cdninfo.apiAction + "&purge_domain=" + apiPurgeDomain + "&purge_url=" + apiFtpPath;
                                        log.logging("cdnapi : " + cdnapi);
                                        cdnresponse = Http.Get(cdnapi);
                                        log.logging("cdnresponse : " + cdnresponse);
                                        mapper.UpdateCDNURL(ftpInfo.type, full_url, ftpInfo.clip_pk);
                                    }
                                    
                                    if (!String.IsNullOrEmpty(ftpInfo.cid) && ftpInfo.type.ToLower().Equals("yt_srt"))
                                    {
                                        //유튜브 자막파일 SRT
                                        apiPurgeDomain = "http://Mov.mbcmpp.co.kr";
                                        cdnapi = cdninfo.apiDomain + "?user_id=" + cdninfo.apiUserid + "&passwd=" + cdninfo.apiPasswd + "&action=" + cdninfo.apiAction + "&purge_domain=" + apiPurgeDomain + "&purge_url=" + apiFtpPath;
                                        log.logging("cdnapi : " + cdnapi);
                                        cdnresponse = Http.Get(cdnapi);
                                        log.logging("cdnresponse : " + cdnresponse);
                                        mapper.Update_YT_CDNURL(ftpInfo.type, full_url, ftpInfo.cid);
                                    }

                                    if (!String.IsNullOrEmpty(ftpInfo.cid) && ftpInfo.type.ToLower().Equals("yt_srt_ko"))
                                    {
                                        //유튜브 한국어 언어 파일 SRT
                                        apiPurgeDomain = "http://Mov.mbcmpp.co.kr";
                                        cdnapi = cdninfo.apiDomain + "?user_id=" + cdninfo.apiUserid + "&passwd=" + cdninfo.apiPasswd + "&action=" + cdninfo.apiAction + "&purge_domain=" + apiPurgeDomain + "&purge_url=" + apiFtpPath;
                                        log.logging("cdnapi : " + cdnapi);
                                        cdnresponse = Http.Get(cdnapi);
                                        log.logging("cdnresponse : " + cdnresponse);
                                        mapper.Update_YT_ITEM_CDNURL(full_url, ftpInfo.cid, "ko");
                                    }

                                    if (!String.IsNullOrEmpty(ftpInfo.cid) && ftpInfo.type.ToLower().Equals("yt_srt_ja"))
                                    {
                                        //유튜브 일본어 언어 파일 SRT
                                        apiPurgeDomain = "http://Mov.mbcmpp.co.kr";
                                        cdnapi = cdninfo.apiDomain + "?user_id=" + cdninfo.apiUserid + "&passwd=" + cdninfo.apiPasswd + "&action=" + cdninfo.apiAction + "&purge_domain=" + apiPurgeDomain + "&purge_url=" + apiFtpPath;
                                        log.logging("cdnapi : " + cdnapi);
                                        cdnresponse = Http.Get(cdnapi);
                                        log.logging("cdnresponse : " + cdnresponse);
                                        mapper.Update_YT_ITEM_CDNURL(full_url, ftpInfo.cid, "ja");
                                    }

                                    if (!String.IsNullOrEmpty(ftpInfo.cid) && ftpInfo.type.ToLower().Equals("yt_srt_en"))
                                    {
                                        //유튜브 영어 언어 파일 SRT
                                        apiPurgeDomain = "http://Mov.mbcmpp.co.kr";
                                        cdnapi = cdninfo.apiDomain + "?user_id=" + cdninfo.apiUserid + "&passwd=" + cdninfo.apiPasswd + "&action=" + cdninfo.apiAction + "&purge_domain=" + apiPurgeDomain + "&purge_url=" + apiFtpPath;
                                        log.logging("cdnapi : " + cdnapi);
                                        cdnresponse = Http.Get(cdnapi);
                                        log.logging("cdnresponse : " + cdnresponse);
                                        mapper.Update_YT_ITEM_CDNURL(full_url, ftpInfo.cid, "en");
                                    }

                                    if (!String.IsNullOrEmpty(ftpInfo.cid) && ftpInfo.type.ToLower().Equals("yt_srt_zh"))
                                    {
                                        //유튜브 중국어 언어 파일 SRT
                                        apiPurgeDomain = "http://Mov.mbcmpp.co.kr";
                                        cdnapi = cdninfo.apiDomain + "?user_id=" + cdninfo.apiUserid + "&passwd=" + cdninfo.apiPasswd + "&action=" + cdninfo.apiAction + "&purge_domain=" + apiPurgeDomain + "&purge_url=" + apiFtpPath;
                                        log.logging("cdnapi : " + cdnapi);
                                        cdnresponse = Http.Get(cdnapi);
                                        log.logging("cdnresponse : " + cdnresponse);
                                        mapper.Update_YT_ITEM_CDNURL(full_url, ftpInfo.cid, "zh");
                                    }

                                    if (!String.IsNullOrEmpty(ftpInfo.clip_pk) && ftpInfo.type.ToLower().Equals("img"))
                                    {
                                        //클립 이미지
                                        // 1. 이미지 퍼지
                                        // 2. full_url 업데이트
                                        //apiPurgeDomain = "http://mbcplus-dn.dl.cdn.cloudn.co.kr";                                
                                        apiPurgeDomain = "http://Img.mbcmpp.co.kr";
                                        cdnapi = cdninfo.apiDomain + "?user_id=" + cdninfo.apiUserid + "&passwd=" + cdninfo.apiPasswd + "&action=" + cdninfo.apiAction + "&purge_domain=" + apiPurgeDomain + "&purge_url=" + apiFtpPath;
                                        log.logging("cdnapi : " + cdnapi);
                                        cdnresponse = Http.Get(cdnapi);
                                        log.logging("cdnresponse : " + cdnresponse);
                                        mapper.UpdateCDNURL(ftpInfo.type, full_url, ftpInfo.clip_pk);
                                    }

                                    if (!String.IsNullOrEmpty(ftpInfo.cid) && ftpInfo.type.ToLower().Equals("yt_img"))
                                    {
                                        //유튜브 이미지
                                        // 1. 이미지 퍼지
                                        // 2. full_url 업데이트
                                        //apiPurgeDomain = "http://mbcplus-dn.dl.cdn.cloudn.co.kr";                                
                                        apiPurgeDomain = "http://Img.mbcmpp.co.kr";
                                        cdnapi = cdninfo.apiDomain + "?user_id=" + cdninfo.apiUserid + "&passwd=" + cdninfo.apiPasswd + "&action=" + cdninfo.apiAction + "&purge_domain=" + apiPurgeDomain + "&purge_url=" + apiFtpPath;
                                        log.logging("cdnapi : " + cdnapi);
                                        cdnresponse = Http.Get(cdnapi);
                                        log.logging("cdnresponse : " + cdnresponse);
                                        mapper.Update_YT_CDNURL(ftpInfo.type, full_url, ftpInfo.cid);
                                    }
                                    // check cdn_url 이미지. 영상 체크
                                    if (mapper.CheckUrlisCompleted(ftpInfo.clip_pk))
                                    {   
                                        mapper.UpdateClipStatus(ftpInfo.cid, "Completed");                                        
                                    }
                                    if (String.Equals(ftpInfo.attribute, "PA") && ftpInfo.type.ToLower().Equals("img"))
                                    {
                                        //프로그램 이미지일 경우
                                        //apiPurgeDomain = "http://mbcplus-dn.dl.cdn.cloudn.co.kr";
                                        apiPurgeDomain = "http://Img.mbcmpp.co.kr";
                                        cdnapi = cdninfo.apiDomain + "?user_id=" + cdninfo.apiUserid + "&passwd=" + cdninfo.apiPasswd + "&action=" + cdninfo.apiAction + "&purge_domain=" + apiPurgeDomain + "&purge_url=" + apiFtpPath;
                                        log.logging("cdnapi : " + cdnapi);
                                        cdnresponse = Http.Get(cdnapi);
                                        log.logging("cdnresponse : " + cdnresponse);

                                        frmMain.WriteLogThread("[FTPService] 프로그램 id 이미지 업데이트 : " + ftpInfo.pid);
                                        log.logging("프로그램 이미지 url : " + full_url);
                                        mapper.UpdateProgramImgCompleted(full_url, ftpInfo.pid, ftpInfo.program_img_type);
                                        // 프로그램 이미지 업데이트 완료(메타허브 업데이트와 관계없으므로 Completed 처리함
                                    }
                                    if ( (String.Equals(ftpInfo.attribute, "A1") || String.Equals(ftpInfo.attribute,"A0")) && ftpInfo.type.ToLower().Equals("img"))
                                    {
                                        //프로그램 이미지일 경우
                                        //apiPurgeDomain = "http://mbcplus-dn.dl.cdn.cloudn.co.kr";
                                        apiPurgeDomain = "http://Img.mbcmpp.co.kr";
                                        cdnapi = cdninfo.apiDomain + "?user_id=" + cdninfo.apiUserid + "&passwd=" + cdninfo.apiPasswd + "&action=" + cdninfo.apiAction + "&purge_domain=" + apiPurgeDomain + "&purge_url=" + apiFtpPath;
                                        log.logging("cdnapi : " + cdnapi);
                                        cdnresponse = Http.Get(cdnapi);
                                        log.logging("cdnresponse : " + cdnresponse);

                                        frmMain.WriteLogThread("[FTPService] SMR 프로그램 id 이미지 업데이트 : " + ftpInfo.smr_pid);
                                        log.logging("SMR 프로그램 이미지 url : " + full_url);
                                        mapper.UpdateSmrProgramImgCompleted(full_url, ftpInfo.smr_pid, ftpInfo.smr_img_type);
                                        // 프로그램 이미지 업데이트 완료(메타허브 업데이트와 관계없으므로 Completed 처리함
                                    }
                                    if (!String.IsNullOrEmpty(ftpInfo.gid) && String.IsNullOrEmpty(ftpInfo.cid) && ftpInfo.type.ToLower().Equals("img"))
                                    {
                                        //회차 이미지
                                        //apiPurgeDomain = "http://mbcplus-dn.dl.cdn.cloudn.co.kr";                                
                                        apiPurgeDomain = "http://Img.mbcmpp.co.kr";
                                        cdnapi = cdninfo.apiDomain + "?user_id=" + cdninfo.apiUserid + "&passwd=" + cdninfo.apiPasswd + "&action=" + cdninfo.apiAction + "&purge_domain=" + apiPurgeDomain + "&purge_url=" + apiFtpPath;
                                        log.logging("cdnapi : " + cdnapi);
                                        cdnresponse = Http.Get(cdnapi);
                                        log.logging("cdnresponse : " + cdnresponse);

                                        // 회차 이미지 업데이트
                                        frmMain.WriteLogThread("[FTPService] 회차 이미지 업데이트 되었습니다. : " + ftpInfo.gid);
                                        mapper.UpdateProgramSeqCompleted(full_url, ftpInfo.gid, ftpInfo.type);

                                        mapper.UpdateProgramSeqStatus(ftpInfo.gid, "Completed");
                                    }

                                    if (!String.IsNullOrEmpty(ftpInfo.gid) && String.IsNullOrEmpty(ftpInfo.cid) && ftpInfo.type.ToLower().Equals("cue"))
                                    {
                                        //회차 큐시트 파일
                                        apiPurgeDomain = "http://Mov.mbcmpp.co.kr";
                                        cdnapi = cdninfo.apiDomain + "?user_id=" + cdninfo.apiUserid + "&passwd=" + cdninfo.apiPasswd + "&action=" + cdninfo.apiAction + "&purge_domain=" + apiPurgeDomain + "&purge_url=" + apiFtpPath;
                                        log.logging("cdnapi : " + cdnapi);
                                        cdnresponse = Http.Get(cdnapi);
                                        log.logging("cdnresponse : " + cdnresponse);
                                        mapper.UpdateProgramSeqCompleted(full_url, ftpInfo.gid, ftpInfo.type);
                                        mapper.UpdateProgramSeqStatus(ftpInfo.gid, "Completed");
                                    }

                                    if (!String.IsNullOrEmpty(ftpInfo.gid) && String.IsNullOrEmpty(ftpInfo.cid) && ftpInfo.type.ToLower().Equals("script"))
                                    {
                                        //회차 대본 파일
                                        apiPurgeDomain = "http://Mov.mbcmpp.co.kr";
                                        cdnapi = cdninfo.apiDomain + "?user_id=" + cdninfo.apiUserid + "&passwd=" + cdninfo.apiPasswd + "&action=" + cdninfo.apiAction + "&purge_domain=" + apiPurgeDomain + "&purge_url=" + apiFtpPath;
                                        log.logging("cdnapi : " + cdnapi);
                                        cdnresponse = Http.Get(cdnapi);
                                        log.logging("cdnresponse : " + cdnresponse);

                                        mapper.UpdateProgramSeqCompleted(full_url, ftpInfo.gid, ftpInfo.type);
                                        mapper.UpdateProgramSeqStatus(ftpInfo.gid, "Completed");
                                    }

                                }

                                if (ftpInfo.transcoding_YN == "Y") // 현재 customer_id : 1 (BBMC만 transcode_YN = Y)
                                {
                                    String transcoding_serviceType = "";
                                    JObject obj;
                                    //type이 MOV일 경우 Transcoding 명령 날림
                                    if (ftpInfo.type.ToLower().Equals("mov"))
                                    {
                                        // 2면 1M, 2M, bypass 5면 bypass
                                        // 2018-01-04 BBMC로 보낼 경우 type1 만 선택되어야 함. type1은 720p1M, 720p2M
                                        transcoding_serviceType = "type1";

                                        //strRequest = strBBMCHost + "Transform.svc/AddTransform?siteid=1&filepath=" + "/" + m_targetpath + "/" + m_targetfilename + "&servicetype=" + transcoding_serviceType + "&callbackurl=" + m_s_ip4addr;
                                        strRequest = String.Format(@"{0}/api/ejob/add/mbcplus/mbcplus/json?encset={1}&pathfile={2}/{3}", strBBMCHost, transcoding_serviceType, ftpInfo.targetpath, ftpInfo.targetfilename);
                                        strResponse = Http.Get(strRequest);

                                        log.logging(strRequest);
                                        log.logging(strResponse);

                                        obj = JObject.Parse(strResponse);
                                        tc_pk = obj["requestId"].ToString();
                                        frmMain.WriteLogThread(String.Format(@"[FTPService] tc_pk({0}), ftp_pk({1}) MOV", tc_pk, ftpInfo.pk));

                                        mapper.UpdateTranscodePK(tc_pk, ftpInfo.pk);
                                        // Clip상태를 Waiting Callback 으로 변경
                                        //mapper.UpdateSetWaitingCallBack(clip_pk);
                                        mapper.UpdateClipStatus(ftpInfo.cid, "Waiting Callback");
                                    }
                                    else if (ftpInfo.type.ToLower().Equals("img"))
                                    {
                                        // transcode_YN = Y면서 img는 2018-1-4일 부로 deprecated
                                        /*
                                        transcoding_serviceType = "type5"; // Notranscoding
                                        //strRequest = strBBMCHost + "Transform.svc/AddTransform?siteid=1&filepath=" + "/" + m_targetpath + "/" + m_targetfilename + "&servicetype=5&callbackurl=" + m_s_ip4addr;
                                        strRequest = String.Format(@"{0}/api/ejob/add/mbcplus/mbcplus/json?encset={1}&pathfile={2}/{3}", strBBMCHost, "type5", m_targetpath, m_targetfilename);
                                        strResponse = Http.Get(strRequest);

                                        log.logging(strRequest);
                                        log.logging(strResponse);

                                        JObject cdnonj = JObject.Parse(strResponse);
                                        JObject obj = JObject.Parse(strResponse);
                                        m_tc_pk = obj["requestId"].ToString();
                                        frmMain.WriteLogThread(String.Format(@"tc_pk({0}), ftp_pk({1}) IMG", m_tc_pk, m_pk));

                                        m_sql = String.Format("UPDATE TB_FTP_QUEUE SET tc_pk = '{0}' WHERE ftp_queue_pk = '{1}'", m_tc_pk, m_pk);
                                        connPool.ConnectionOpen();
                                        cmd = new MySqlCommand(m_sql, connPool.getConnection());
                                        cmd.ExecuteNonQuery();
                                        connPool.ConnectionClose();
                                        */
                                    }
                                }
                            }
                            else
                            {
                                //Failed
                                mapper.UpdateFtpStatus(ftpInfo.pk, "Failed", errmsg);                                
                                if (!String.IsNullOrEmpty(ftpInfo.gid) && String.IsNullOrEmpty(ftpInfo.cid) )
                                { //회차일 경우 회차 실패
                                    mapper.UpdateProgramSeqStatus(ftpInfo.gid, "Failed", "FTP 전송 중 오류");
                                }
                                if (!String.IsNullOrEmpty(ftpInfo.cid) && !ftpInfo.type.ToLower().Equals("xml"))
                                { //클립일 경우 클립 실패
                                    mapper.UpdateClipStatus(ftpInfo.cid, "Failed");
                                } else
                                {
                                    log.logging("FTP Sending Failed But status not change : " + ftpInfo.type);
                                }
                            }
                        }
                        catch (Exception e)
                        {                            
                            frmMain.WriteLogThread("[FTPService] " + e.ToString());
                            log.logging(e.ToString());
                            //ftp 상태 실패
                            mapper.UpdateFtpStatus(ftpInfo.pk, "Failed", Util.escapedPath(e.ToString()));
                            if ( !String.IsNullOrEmpty(ftpInfo.gid) && String.IsNullOrEmpty(ftpInfo.cid) )
                            { //회차일 경우 회차 실패
                                mapper.UpdateProgramSeqStatus(ftpInfo.gid, "Failed", e.ToString());
                            }
                            if ( !String.IsNullOrEmpty(ftpInfo.cid) && !ftpInfo.type.ToLower().Equals("xml"))
                            { //클립일 경우 클립 실패
                                mapper.UpdateClipStatus(ftpInfo.cid, "Failed");
                            } else
                            {
                                log.logging("FTP Sending Failed But status not change  : " + ftpInfo.type + "\n" + e.ToString());
                            }
                        }
                    }
                    ds.Clear();
                }
                catch (Exception e)
                {
                    //frmMain.WriteLogThread("[FTPService] " + e.ToString());
                    log.logging(e.ToString());
                    log.logging(e.StackTrace.ToString());
                }
                Thread.Sleep(1000);
            }
            connPool.ConnectionDisPose();
            log.logging("Thread Terminate");
        }
    }
}