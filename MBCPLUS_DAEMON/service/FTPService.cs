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

        void initializeDailyMotion()
        {
            SqlMapper mapper = new SqlMapper();
            String responseString = null;
            try
            {
                responseString = DMInfo.getChannelList();

                // 성공시 Completed
                JObject obj = JObject.Parse(responseString);
                JArray jarray = (JArray)obj["list"];
                DMChannelList dmlist = new DMChannelList();

                foreach (JObject o in jarray)
                {
                    dmlist.id = o["id"].ToString();
                    dmlist.name = o["name"].ToString();
                    dmlist.description = o["description"].ToString();
                    mapper.InsertDMChannelList(dmlist);
                }
            }
            catch(Exception e)
            {
                log.logging(e.ToString());
            }
        }

        void ininitializeAPI()
        {
            Thread.Sleep(5000);
            SqlMapper mapper = new SqlMapper();
            JObject obj = null;
            String response = null;
            response = DMInfo.getPlaylist();
            obj = JObject.Parse(response);
            JArray items = (JArray)obj["list"];
            for(int i=0;i< items.Count ;i++)
            {
                if(mapper.SetDMPlayList(items[i]["id"].ToString(), items[i]["name"].ToString(), i)) {

                } else
                {
                    log.logging("SetDMPlayList Failed : " + items[i]["id"].ToString());
                }
            }
            List<Dictionary<String, String>> list = new List<Dictionary<String, String>>();            
            YTInfo ytInfo = Singleton.getInstance().Get_YTInstance();
            //ytInfo.UpdateVideoInfo("2vsHxhgULW4");
            ytInfo.authentication("mbcplus2.json");
            //ytInfo.authentication("mbcplus3.json");
            ytInfo.GetChannelList();
            DataSet ds = new DataSet();
            mapper.GetYTChannelList(ds);

            foreach (DataRow r in ds.Tables[0].Rows)
            {
                list = ytInfo.GetPlayList(r["id"].ToString());
                for (int i=0; i <list.Count;i++)
                {
                    mapper.SETYTPlayList(list[i]["id"], list[i]["name"], r["id"].ToString() ,i);
                }
            }
            ds.Clear();
        }

        void DoWork()
        {
            log = new Log(this.GetType().Name);
            Thread t1 = new Thread(() => Run("normal"));
            Thread t2 = new Thread(() => Run("yt"));
            Thread t3 = new Thread(() => Run("dm"));
            t1.Start();
            Thread.Sleep(1000);
            t2.Start();
            Thread.Sleep(1000);
            t3.Start();

            ininitializeAPI();
            initializeDailyMotion();
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
            // m_멤버변수 아님 지역 변수임 안정화 이후 변수명 바꿀 예정
            String m_srcpath = null;
            String m_targetpath = null;
            String m_targetfilename = null;
            String m_customer_id = null;
            String m_customer_name = null;
            String m_host = null;
            String m_id = null;
            String m_pw = null;
            String m_port = null;
            String m_path = null;
            String m_transcoding_YN = null;
            String m_alias_YN = null;
            String m_clip_YN = null;
            String m_s_title = null;
            String m_title = null;
            String m_broaddate = null;
            String m_sportskind = null;
            String m_team1 = null;
            String m_team2 = null;
            String m_inning = null;

            String m_pk = null;
            String m_sql = null;
            String m_clip_pk = null;
            String m_tc_pk = null;
            String m_pid = null;
            String m_gid = null;
            String cid = null;
            String m_metahub_YN = null;
            String m_s_metahub_YN = null;
            String m_cdn_img = null;
            String m_cdn_mov = null;
            String m_s_ip4addr = null;

            String yt_videoid = null;
            String yt_channel_id = null;
            String yt_playlist_id = null;
            String yt_title = null;
            String yt_description = null;
            String yt_tags = null;
            String yt_isuse = null;
            String yt_category = null;
            String yt_enable_contentid = null;
            String yt_uploader_name = null;
            String yt_upload_control_id = null;
            String yt_usage_policy = null;
            String yt_match_policy = null;
            String yt_start_time = null;
            String yt_spoken_language = null;
            String yt_target_language = null;
            String yt_custom_id = null;

            String dm_videoid = null;
            String dm_title = null;
            String dm_description = null;
            String dm_category = null;
            String dm_publish_date = null;
            String dm_expiry_date = null;
            String dm_url = null;
            String dm_tag = null;
            String dm_isuse = null;
            String dm_playlistid = null;

            //YTInfo ytInfo = Singleton.getInstance().Get_YTInstance();           

            ConnectionPool connPool;
            SqlMapper mapper = new SqlMapper();
            String status = null;
            String type = null;
            MySqlCommand cmd;
            connPool = new ConnectionPool();
            connPool.SetConnection(new MySqlConnection(Singleton.getInstance().GetStrConn()));

            //MySqlDataAdapter adpt;

            //String strBaseUri = "http://metaapi.mbcmedia.net:5000/SMRMetaCollect.svc/";
            String strBBMCHost = "http://mbcplus.mediacast.co.kr/";
            // 2017-12-13 변경됨 // softcoding으로 바꿀 것
            strBBMCHost = "http://mbcplus2.mediacast.co.kr";

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
            //frmMain.WriteLogThread("FTP Service Start...");


            while (!_shouldStop)
            {                
                try
                {
                    DataSet ds = new DataSet();
                    String[] callbackurl;
                    callbackurl = Singleton.getInstance().GetStrCalalbackURL();
                    lock (lockObject)
                    {
                        mapper.GetFtpInfo(ds, sql_tail);
                    }
                    
                    // Rows 가 1 이상일 때 탐색함
                    foreach (DataRow r in ds.Tables[0].Rows)
                    {
                        try
                        {
                            m_pk = r["pk"].ToString();
                            m_clip_pk = r["clip_pk"].ToString();
                            m_customer_id = r["customer_id"].ToString();
                            m_srcpath = r["srcpath"].ToString();
                            m_targetpath = r["targetpath"].ToString();
                            m_targetfilename = r["targetfilename"].ToString();
                            m_customer_name = r["customer_name"].ToString();
                            m_transcoding_YN = r["transcoding_YN"].ToString();
                            m_alias_YN = r["alias_YN"].ToString();
                            m_s_title = r["s_title"].ToString();
                            m_title = r["title"].ToString();
                            m_broaddate = r["broaddate"].ToString();
                            m_sportskind = r["sportskind"].ToString();
                            m_team1 = r["team1"].ToString();
                            m_team2 = r["team2"].ToString();
                            m_inning = r["inning"].ToString();
                            m_host = r["host"].ToString();
                            m_port = r["port"].ToString();
                            m_path = r["path"].ToString();
                            m_id = r["id"].ToString();
                            m_pw = r["pw"].ToString();
                            status = r["status"].ToString();
                            type = r["type"].ToString();
                            m_clip_YN = r["clip_YN"].ToString();
                            m_pid = r["pid"].ToString();
                            m_gid = r["gid"].ToString();
                            cid = r["cid"].ToString();
                            m_metahub_YN = r["metahub_YN"].ToString();
                            m_s_metahub_YN = r["s_metahub_YN"].ToString();
                            m_cdn_img = r["cdnurl_img"].ToString();
                            m_cdn_mov = r["cdnurl_mov"].ToString();
                            m_s_ip4addr = callbackurl[0]; // NAT 첫번째 값으로 고정 2017-12-13

                            yt_videoid = r["yt_videoid"].ToString();
                            yt_channel_id = r["yt_channel_id"].ToString();
                            yt_playlist_id = r["yt_playlist_id"].ToString();
                            yt_title = r["yt_title"].ToString();
                            yt_description = r["yt_description"].ToString();
                            yt_tags = r["yt_tag"].ToString();
                            yt_isuse = r["yt_isuse"].ToString();
                            yt_category = r["yt_category"].ToString();                                
                            yt_enable_contentid = r["yt_enable_contentid"].ToString();
                            yt_uploader_name = r["yt_uploader_name"].ToString();
                            yt_upload_control_id = r["yt_upload_control_id"].ToString();
                            yt_usage_policy = r["yt_usage_policy"].ToString();
                            yt_match_policy = r["yt_match_policy"].ToString();
                            yt_start_time = r["yt_start_time"].ToString();
                            yt_spoken_language = r["yt_spoken_language"].ToString();
                            yt_target_language = r["yt_target_language"].ToString();
                            yt_custom_id = r["yt_custom_id"].ToString();

                            dm_videoid = r["dm_videoid"].ToString();
                            dm_title = r["dm_title"].ToString();
                            dm_description = r["dm_description"].ToString();
                            dm_category = r["dm_category"].ToString();
                            dm_publish_date = r["dm_publish_date"].ToString();
                            dm_expiry_date = r["dm_expiry_date"].ToString();
                            dm_url = r["dm_url"].ToString();
                            dm_tag = r["dm_tag"].ToString();
                            dm_isuse = r["dm_isuse"].ToString();
                            dm_playlistid = r["dm_playlistid"].ToString();

                            YTInfo ytInfo = new YTInfo();
                            DMInfo dmInfo = new DMInfo();

                            ytInfo.cid = cid;
                            ytInfo.videoid = yt_videoid;
                            ytInfo.channelid = yt_channel_id;
                            ytInfo.playlist_id = yt_playlist_id;
                            // youtube playlist 형식으로 변환
                            ytInfo.playlist_id = ytInfo.playlist_id.Replace(",", "|");
                            ytInfo.title = yt_title;
                            ytInfo.description = yt_description;
                            ytInfo.tags = yt_tags;
                            ytInfo.isuse = yt_isuse;
                            ytInfo.category = yt_category;
                            ytInfo.enable_contentid = yt_enable_contentid;
                            ytInfo.uploader_name = yt_uploader_name;
                            ytInfo.upload_control_id = yt_upload_control_id;
                            ytInfo.usage_policy = yt_usage_policy;
                            ytInfo.match_policy = yt_match_policy;
                            ytInfo.start_time = yt_start_time;
                            ytInfo.spoken_language = yt_spoken_language;
                            ytInfo.target_language = yt_target_language;
                            ytInfo.custom_id = yt_custom_id;

                            dmInfo.cid = cid;
                            dmInfo.videoid = dm_videoid;
                            dmInfo.title = dm_title;
                            dmInfo.description = dm_description;
                            dmInfo.category = dm_category;
                            dmInfo.publish_date = dm_publish_date;
                            dmInfo.expiry_date = dm_expiry_date;
                            dmInfo.url = dm_url;
                            dmInfo.tags = dm_tag;
                            dmInfo.isuse = dm_isuse;
                            dmInfo.playlistid = dm_playlistid;
                            dmInfo.geoblock_code = r["geoblock_code"].ToString();
                            dmInfo.geoblock_value = r["geoblock_value"].ToString();

                            //frmMain.WriteLogThread("[FTPService] CallbackURL : " + m_s_ip4addr);
                            
                            // 해피라이징의 경우 clip_YN 이 N으로 되어있으며 MOV 의 경우 트랜스코딩 후 파일은 보내지 않고 XML로 경로만 보냄
                            if (String.Equals("N", m_clip_YN) && String.Equals("MOV", type))
                            {
                                mapper.UpdateContentBypass(m_pk);
                                frmMain.WriteLogThread(String.Format(@"[FTPService] {0} clip is bypassed", m_clip_pk));
                                continue;
                            }

                            // 파일이름을 변경해서 보내야할 고객사의 경우는 아래의 로직을 따름
                            if (m_alias_YN == "Y")
                            {
                                String alias_targetpath = "";
                                String alias_targetfilename = "";

                                alias_targetpath = String.Format(@"{0}/{1}", m_broaddate, Util.escapedPath(Util.repaceInvalidFilename(m_s_title)));
                                alias_targetfilename = String.Format(@"{0}_{1}_vs_{2}_{3}_{4}{5}", m_broaddate, m_team1, m_team2, m_inning, Util.escapedPath(Util.repaceInvalidFilename(m_title)), Path.GetExtension(Util.repaceInvalidFilename(m_targetfilename)));

                                mapper.UpdateAliasFilepath(m_pk, alias_targetpath, alias_targetfilename);
                                
                                m_targetpath = alias_targetpath.Replace("''", "'");
                                m_targetfilename = alias_targetfilename.Replace("''", "'");
                            }

                            // 파일 타겟 네임 설정 부분
                            FTPMgr ftpmgr = new FTPMgr(connPool);
                            ftpmgr.SetAlias(m_alias_YN);
                            ftpmgr.SetConfidential(m_pk, m_host, m_path, m_id, m_pw, m_port, m_customer_id, m_gid, cid);
                            ftpmgr.SetSourcePath(m_srcpath);
                            ftpmgr.SetTargetPath(m_targetpath + "/" + m_targetfilename);

                            Dictionary<String, Object> map = new Dictionary<String, Object>();

                            map = r.Table.Columns
                                .Cast<DataColumn>()
                                .ToDictionary(col => col.ColumnName, col => r.Field<Object>(col.ColumnName));

                            if (type.ToLower() == "xml")
                            {
                                // 전송해야할 대상이 XML이라면 이 시점에서 XML 생성
                                // HLS, RTMP, download url 이 빠진 xml을 생성
                                MBCPlusMeta mbcplusMeta = new MBCPlusMeta();
                                mbcplusMeta.MakeXML(map);

                                XmlDocument xmlDoc = mbcplusMeta.GetCurrentXmlDocument();
                                xmlDoc.Save(m_srcpath);
                            }
                            String errmsg = "";
                            if (m_customer_id == "9")
                            {
                                // 채널 리스트 갱신
                                // 플레이 리스트 갱신
                                // 저작권 차단 목록 갱신
                                String session_id = mapper.GetYoutubeSessionID(cid);                                
                                if ( String.IsNullOrEmpty(session_id) )
                                {
                                    ytInfo.session_id = DateTime.Now.Ticks.ToString("x");
                                    mapper.UpdateYoutubeSessionID(cid, ytInfo.session_id);
                                }
                                else
                                {
                                    ytInfo.session_id = session_id;
                                }                                
                                if (type.ToLower() == "xml")
                                {
                                    mapper.UpdateContentBypass(m_pk);
                                }
                                else if (ftpmgr.SendFile("yt", type, ytInfo))
                                {
                                    frmMain.WriteLogThread(String.Format(@"[FTPService] (Youtube) {0} is completed", m_pk));
                                    //make csvfile
                                    if (ytInfo.IsMovCompleted)
                                    {
                                        String thumb_Filename = null;
                                        ytInfo.IsImgCompleted = mapper.YTimgUploadCheck(cid, out thumb_Filename);
                                        if ( !String.IsNullOrEmpty(thumb_Filename) )
                                        {
                                            ytInfo.custom_thumbnail = thumb_Filename;
                                        }
                                        String csvFileName = ytInfo.MakeYoutubeCSVFile();
                                        String csv_destPath = String.Format("/{0}_{1}/{2}", Path.GetFileNameWithoutExtension(csvFileName), ytInfo.session_id, Path.GetFileName(csvFileName));
                                        frmMain.WriteLogThread(String.Format("{0} is created", csvFileName));
                                        ftpmgr.yt_csvSendFile(csvFileName, csv_destPath);
                                        ftpmgr.yt_deliveryCompleteSendFile(csv_destPath);
                                        //Youtube service ready to parse
                                        mapper.UpdateYoutubeReady(cid);
                                    }
                                    mapper.UpdateFtpCompleted(m_pk);

                                    //thumbnail과 mov의 업로드가 되었는지 체크
                                    //Delevery.Completed 전송함
                                    //status xml 생성을 기다릴 수 있게 YoutubeService로 넘김
                                }
                            }
                            else if (m_customer_id == "10")
                            {
                                if (type.ToLower() == "img" | type.ToLower() == "xml")
                                {
                                    mapper.UpdateContentBypass(m_pk);
                                }
                                else
                                {
                                    if (ftpmgr.SendDailymotion(type, dmInfo))
                                    {
                                        //업로드 후 만약 비디오 아이디가 존재 하면 컨텐츠 삭제 후 videoid 공백으로 변경
                                        if (!String.IsNullOrEmpty(dmInfo.videoid))
                                        {
                                            //기존 비디오 삭제 (잠시 보류)
                                            String response = null;
                                            dmInfo.DeleteVideo(dmInfo.videoid, out response);
                                            frmMain.WriteLogThread(String.Format("Dailymotion videoid({0} is deleted)", dmInfo.videoid));
                                            log.logging(String.Format("Dailymotion videoid({0} is deleted) response : {1}", dmInfo.videoid, response));
                                        }
                                        mapper.UpdateFtpCompleted(m_pk);
                                        frmMain.WriteLogThread(String.Format(@"[FTPService] (Dailymotion) {0} is completed", m_pk));
                                        mapper.UpdateClipStatus(cid, "Completed");
                                    }
                                    else
                                    {
                                        mapper.UpdateFtpFailed(m_pk);
                                        mapper.UpdateClipStatus(cid, "Failed");
                                        frmMain.WriteLogThread(String.Format(@"[FTPService] (Dailymotion) {0} is Failed", m_pk));
                                    }
                                }
                            }
                            else if (ftpmgr.SendFile(out errmsg))
                            {
                                // 파일전송이 완료 되어 상태를 Completed 으로 변경
                                mapper.UpdateFtpCompleted(m_pk);
                                frmMain.WriteLogThread(String.Format(@"[FTPService] {0} is completed", m_pk));

                                String strRequest = "";
                                String strResponse = "";

                                // customer_id : 2 LG_CDN 일 경우 full_path를 요청함
                                if (m_customer_id == "2")
                                {
                                    String full_url = "";
                                    String rtmp_url = "";
                                    String hls_url = "";

                                    GetFullPathUrl(cdninfo, type, out full_url, out rtmp_url, out hls_url, m_targetpath, m_targetfilename);

                                    String apiPurgeDomain;
                                    String cdnapi;
                                    String cdnresponse;
                                    apiFtpPath = m_targetpath + "/" + m_targetfilename;

                                    if (!String.IsNullOrEmpty(m_clip_pk) && type.ToLower().Equals("mov"))
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
                                        mapper.UpdateClipInfos(full_url, rtmp_url, hls_url, m_clip_pk);
                                    }
                                    if (!String.IsNullOrEmpty(m_clip_pk) && type.ToLower().Equals("img"))
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
                                        mapper.UpdateCDNURL(type, full_url, m_clip_pk);
                                    }
                                    // check cdn_url 이미지. 영상 체크
                                    if (mapper.CheckUrlisCompleted(m_clip_pk))
                                    {
                                        /*
                                        // 현 시점의 XML 생성
                                        if (!map.ContainsKey("rtmp_url"))
                                        {
                                            map.Add("rtmp_url", rtmp_url);
                                        }
                                        if (!map.ContainsKey("hls_url"))
                                        {
                                            map.Add("hls_url", hls_url);
                                        }
                                        MBCPlusMeta mbcplusMeta = new MBCPlusMeta();
                                        mbcplusMeta.MakeXML(map);

                                        XmlDocument xmlDoc = mbcplusMeta.GetCurrentXmlDocument();
                                        xmlDoc.Save(m_srcpath);
                                         */

                                        if (m_metahub_YN.Equals("Y"))
                                        {
                                            mapper.UpdateClipReady(m_clip_pk);
                                        }
                                        else
                                        {
                                            mapper.UpdateClipCompleted(m_clip_pk);
                                        }
                                    }
                                    if (!String.IsNullOrEmpty(m_pid) && type.ToLower().Equals("img"))
                                    {
                                        //프로그램 이미지일 경우
                                        //apiPurgeDomain = "http://mbcplus-dn.dl.cdn.cloudn.co.kr";                                
                                        apiPurgeDomain = "http://Img.mbcmpp.co.kr";
                                        cdnapi = cdninfo.apiDomain + "?user_id=" + cdninfo.apiUserid + "&passwd=" + cdninfo.apiPasswd + "&action=" + cdninfo.apiAction + "&purge_domain=" + apiPurgeDomain + "&purge_url=" + apiFtpPath;
                                        log.logging("cdnapi : " + cdnapi);
                                        cdnresponse = Http.Get(cdnapi);
                                        log.logging("cdnresponse : " + cdnresponse);

                                        frmMain.WriteLogThread("[CDNService] 프로그램 id 이미지 업데이트 : " + m_pid);
                                        log.logging("프로그램 이미지 url : " + full_url);
                                        mapper.UpdateProgramImgCompleted(full_url, m_pid);
                                        // 프로그램 이미지 업데이트 완료(메타허브 업데이트와 관계없으므로 Completed 처리함
                                    }
                                    if (!String.IsNullOrEmpty(m_gid) && String.IsNullOrEmpty(cid) && type.ToLower().Equals("img"))
                                    {
                                        //회차 이미지
                                        //apiPurgeDomain = "http://mbcplus-dn.dl.cdn.cloudn.co.kr";                                
                                        apiPurgeDomain = "http://Img.mbcmpp.co.kr";
                                        cdnapi = cdninfo.apiDomain + "?user_id=" + cdninfo.apiUserid + "&passwd=" + cdninfo.apiPasswd + "&action=" + cdninfo.apiAction + "&purge_domain=" + apiPurgeDomain + "&purge_url=" + apiFtpPath;
                                        log.logging("cdnapi : " + cdnapi);
                                        cdnresponse = Http.Get(cdnapi);
                                        log.logging("cdnresponse : " + cdnresponse);

                                        // 회차 이미지 업데이트
                                        frmMain.WriteLogThread("[CDNService] 회차 이미지 확인 : " + m_gid);
                                        mapper.UpdateProgramSeqImg(full_url, m_gid);

                                        // 조건 확인 ** 필수 ** Ready는 메타허브로 메타를 전송할 준비가 되었다는 의미
                                        if (m_s_metahub_YN.Equals("Y"))
                                        {
                                            mapper.UpdateProgramSEQReady(m_gid);
                                        }
                                        else
                                        {
                                            mapper.UpdateProgramSEQCompleted(m_gid);
                                        }
                                    }
                                }

                                if (m_transcoding_YN == "Y") // 현재 customer_id : 1 (BBMC만 transcode_YN = Y)
                                {
                                    String transcoding_serviceType = "";
                                    JObject obj;
                                    //type이 MOV일 경우 Transcoding 명령 날림
                                    if (type.ToLower().Equals("mov"))
                                    {
                                        // 2면 1M, 2M, bypass 5면 bypass
                                        // 2018-01-04 BBMC로 보낼 경우 type1 만 선택되어야 함. type1은 720p1M, 720p2M
                                        transcoding_serviceType = "type1";

                                        //strRequest = strBBMCHost + "Transform.svc/AddTransform?siteid=1&filepath=" + "/" + m_targetpath + "/" + m_targetfilename + "&servicetype=" + transcoding_serviceType + "&callbackurl=" + m_s_ip4addr;
                                        strRequest = String.Format(@"{0}/api/ejob/add/mbcplus/mbcplus/json?encset={1}&pathfile={2}/{3}", strBBMCHost, transcoding_serviceType, m_targetpath, m_targetfilename);
                                        strResponse = Http.Get(strRequest);

                                        log.logging(strRequest);
                                        log.logging(strResponse);

                                        obj = JObject.Parse(strResponse);
                                        m_tc_pk = obj["requestId"].ToString();
                                        frmMain.WriteLogThread(String.Format(@"[FTPService] tc_pk({0}), ftp_pk({1}) MOV", m_tc_pk, m_pk));

                                        mapper.UpdateTranscodePK(m_tc_pk, m_pk);
                                        // Clip상태를 Waiting Callback 으로 변경
                                        mapper.UpdateSetWaitingCallBack(m_clip_pk);
                                    }
                                    else if (type.ToLower().Equals("img"))
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
                                m_sql = String.Format("UPDATE TB_FTP_QUEUE SET endtime = CURRENT_TIMESTAMP(), status = 'Failed', errmsg = '{1}' WHERE ftp_queue_pk = '{0}'", m_pk, errmsg);
                                connPool.ConnectionOpen();
                                //Failed 로 변경
                                cmd = new MySqlCommand(m_sql, connPool.getConnection());
                                cmd.ExecuteNonQuery();
                                connPool.ConnectionClose();                                
                            }
                        }
                        catch (Exception e)
                        {                            
                            frmMain.WriteLogThread("[FTPService] " + e.ToString());
                            log.logging("[FTPService] " + e.ToString());

                            m_sql = String.Format("UPDATE TB_FTP_QUEUE SET endtime = CURRENT_TIMESTAMP(), status = 'Failed', errmsg = '{1}' WHERE ftp_queue_pk = '{0}'", m_pk, Util.escapedPath(e.ToString()));
                            connPool.ConnectionOpen();

                            //Failed 로 변경
                            cmd = new MySqlCommand(m_sql, connPool.getConnection());
                            cmd.ExecuteNonQuery();
                            connPool.ConnectionClose();
                        }
                    }
                    ds.Clear();
                }
                catch (Exception e)
                {
                    frmMain.WriteLogThread("[FTPService] " + e.ToString());
                    log.logging(e.ToString());
                }
                Thread.Sleep(1000);
            }
            connPool.ConnectionDisPose();
            log.logging("Thread Terminate");
        }
    }
}