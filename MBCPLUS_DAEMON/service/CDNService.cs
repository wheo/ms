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
    internal class CDNService
    {
        private Boolean _shouldStop = false;
        private String m_sql = "";
        private Log log;
        private SqlMapper mapper;

        public CDNService()
        {
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

        public void GetFullurl(CdnInfo cdninfo, String pathfilename, out String full_url, out String rtmp_url, out String hls_url)
        {
            String strFTPid = "";
            String ext = Path.GetExtension(pathfilename);
            full_url = "";
            rtmp_url = "";
            hls_url = "";

            if (ext.ToLower() == ".mp4")
            //if (type.ToLower().Equals("mov"))
            {
                strFTPid = "mbcplus_mbcplus-dn"; // mov
            }
            else if (ext.ToLower() == ".jpg" || ext.ToLower() == ".png")
            //else if (type.ToLower().Equals("img"))
            {
                strFTPid = "mbcplus_mbcplus-img"; // img
            }

            JObject metaJson = new JObject(
                            new JProperty("api_request",
                                new JObject(
                                    new JProperty("ftpid", strFTPid),
                                    new JProperty("key", cdninfo.strAPIKey),
                                    new JProperty("file_path", pathfilename))));

            String strJson = "";
            String uri = cdninfo.strCDNHost + cdninfo.strCDNMethod[0];
            strJson = metaJson.ToString();

            log.logging("url : " + uri);
            log.logging("strJson : " + strJson);
            var response = Http.Post(uri, strJson);
            string responseString = Encoding.UTF8.GetString(response);
            log.logging("response : " + responseString);

            String accessCheck = "";
            JObject obj;
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

        private void Run()
        {
            CdnInfo cdninfo = Singleton.getInstance().GetCdnInfo();
            //String apiPasswd = "mlb@2017";
            String apiFtpPath = "";

            String status = null;
            MySqlCommand cmd;

            DataSet ds = new DataSet();
            //Waiting for make winform
            Thread.Sleep(5000);
            //frmMain.WriteLogThread("CDN Service Start...");
            log.logging("Service Start...");

            while (!_shouldStop)
            {
                try
                {
                    mapper.GetCallbakInfo(ds);

                    foreach (DataRow r in ds.Tables[0].Rows)
                    {
                        vo.CallbackInfo callbackInfo = new vo.CallbackInfo();

                        callbackInfo.pk = r["callback_pk"].ToString();
                        callbackInfo.tc_pk = r["tc_pk"].ToString();
                        callbackInfo.program_seq_pk = r["program_seq_pk"].ToString();
                        callbackInfo.clip_pk = r["clip_pk"].ToString();
                        callbackInfo.profile_id = r["profileid"].ToString();
                        callbackInfo.transcode_YN = r["metahub_YN"].ToString();
                        callbackInfo.ftppath = r["ftppath"].ToString();
                        callbackInfo.transcode_YN = r["transcode_YN"].ToString();

                        callbackInfo.pid = r["pid"].ToString();
                        callbackInfo.gid = r["gid"].ToString();
                        callbackInfo.cid = r["cid"].ToString();

                        status = r["status"].ToString();

                        String full_url = "";
                        String rtmp_url = "";
                        String hls_url = "";

                        if (callbackInfo.ftppath.Length > 1)
                        {
                            callbackInfo.ftppath = callbackInfo.ftppath.Substring(1);
                        }

                        callbackInfo.pathfilename = r["pathfilename"].ToString();
                        callbackInfo.encid = r["encid"].ToString();
                        callbackInfo.encset = r["encset"].ToString();

                        if (callbackInfo.pathfilename.Length > 1)
                        {
                            callbackInfo.pathfilename = callbackInfo.pathfilename.Substring(1);
                        }

                        //Running 으로 변경
                        mapper.UpdateCallbackStatus(callbackInfo.pk, "Running");

                        //콜백을 받으면 퍼지
                        // ftppath extention 에 따라 도메인 분기
                        String apiPurgeDomain = "";
                        String cdnresponse = "";
                        String cdnapi = "";

                        //apiFtpPath = ftppath;
                        // m_pathfilename : callback 받은 상대주소 /mbcplus/...../파일이름720p2M.mp4
                        //apiFtpPath = m_pathfilename;
                        apiFtpPath = callbackInfo.pathfilename;

                        GetFullurl(cdninfo, apiFtpPath, out full_url, out rtmp_url, out hls_url);

                        // 영상일 경우 다운로드 퍼지 1번 VOD용 퍼지 API를 추가로 실행
                        if (Path.GetExtension(apiFtpPath).ToLower() == ".mp4")
                        {
                            //apiPurgeDomain = "http://mbcpvod.vod.cdn.cloudn.co.kr"; // 이전 기록만 남김
                            apiPurgeDomain = "http://vod.mbcmpp.co.kr";
                            cdnapi = cdninfo.apiDomain + "?user_id=" + cdninfo.apiUserid + "&passwd=" + cdninfo.apiPasswd + "&action=" + cdninfo.apiAction + "&purge_domain=" + apiPurgeDomain + "&purge_url=" + apiFtpPath;
                            log.logging(cdnapi);
                            cdnresponse = Http.Get(cdnapi);
                            log.logging(cdnresponse);

                            //apiPurgeDomain = "http://mbcplus-dn.dl.cdn.cloudn.co.kr";
                            apiPurgeDomain = "http://mov.mbcmpp.co.kr";
                            cdnapi = cdninfo.apiDomain + "?user_id=" + cdninfo.apiUserid + "&passwd=" + cdninfo.apiPasswd + "&action=" + cdninfo.apiAction + "&purge_domain=" + apiPurgeDomain + "&purge_url=" + apiFtpPath;
                            log.logging("cdnapi : " + cdnapi);
                            cdnresponse = Http.Get(cdnapi);
                            log.logging(cdnresponse);
                        }
                        else if (Path.GetExtension(apiFtpPath).ToLower() == ".jpg" || Path.GetExtension(apiFtpPath).ToLower() == ".png")
                        {
                            //apiPurgeDomain = "http://mbcplus-dn.dl.cdn.cloudn.co.kr";
                            apiPurgeDomain = "http://Img.mbcmpp.co.kr";
                            cdnapi = cdninfo.apiDomain + "?user_id=" + cdninfo.apiUserid + "&passwd=" + cdninfo.apiPasswd + "&action=" + cdninfo.apiAction + "&purge_domain=" + apiPurgeDomain + "&purge_url=" + apiFtpPath;
                            log.logging("cdnapi : " + cdnapi);
                            cdnresponse = Http.Get(cdnapi);
                            log.logging("cdnresponse : " + cdnresponse);
                        }
                        //strCDNMethod = "/cdnservice/downloadpath";

                        mapper.UpdateCallbackURL(callbackInfo.pk, full_url, rtmp_url, hls_url);

                        log.logging(full_url + " encID : " + callbackInfo.encid);

                        //if (m_encid == "notranscoding" || m_encid == "720p1M" || m_encid == "720p2M" || m_encset == "[copy]")
                        if (callbackInfo.encid == "720p1M" || callbackInfo.encid == "720p2M")
                        {
                            if (String.Equals(Path.GetExtension(full_url).ToLower(), ".mp4"))
                            {
                                String profileid = "";

                                if (callbackInfo.encid == "720p1M")
                                {
                                    profileid = "1";
                                }
                                else if (callbackInfo.encid == "720p2M")
                                {
                                    profileid = "3";
                                }
                                // 확장자를 보고 영상파일로 간주
                                String[] downloadField = new String[15];
                                downloadField[1] = "cdnurl_mov_T1";
                                downloadField[3] = "cdnurl_mov_T2";
                                //downloadField[13] = "cdnurl_mov";

                                String[] rtmpField = new String[15];
                                rtmpField[1] = "rtmp_url_T1";
                                rtmpField[3] = "rtmp_url_T2";
                                //rtmpField[13] = "rtmp_url";

                                String[] hlsField = new String[15];
                                hlsField[1] = "hls_url_T1";
                                hlsField[3] = "hls_url_T2";
                                //hlsField[13] = "hls_url";

                                m_sql = String.Format(@"UPDATE TB_CLIP
                                                        SET {0} = '{1}'
                                                        , {2} = '{3}'
                                                        , {4} = '{5}'
                                                        WHERE cid = '{6}'"
                                    , downloadField[Int32.Parse(profileid)]
                                    , full_url
                                    , rtmpField[Int32.Parse(profileid)]
                                    , rtmp_url
                                    , hlsField[Int32.Parse(profileid)]
                                    , hls_url
                                    , callbackInfo.cid);

                                using (MySqlConnection conn = new MySqlConnection(Singleton.getInstance().GetStrConn()))
                                {
                                    conn.Open();
                                    cmd = new MySqlCommand(m_sql, conn);
                                    cmd.ExecuteNonQuery();
                                }

                                //if (m_profileid == "1" || m_profileid == "3")
                                if (callbackInfo.encid == "720p1M" || callbackInfo.encid == "720p2M")
                                {
                                    // 1M or 2M callback이 올 경우에 처리
                                    m_sql = String.Format(@"UPDATE TB_CLIP
                                                            SET callback_cnt = callback_cnt + 1
                                                            WHERE cid = '{0}'", callbackInfo.cid);
                                    using (MySqlConnection conn = new MySqlConnection(Singleton.getInstance().GetStrConn()))
                                    {
                                        conn.Open();
                                        cmd = new MySqlCommand(m_sql, conn);
                                        cmd.ExecuteNonQuery();
                                    }
                                    //cid,gid,typeid를 TB_CLIP_INFO에 삽입

                                    String typeid = "";
                                    if (callbackInfo.encid == "720p1M")
                                    {
                                        typeid = "3";
                                    }
                                    else if (callbackInfo.encid == "720p2M")
                                    {
                                        typeid = "2";
                                    }
                                    // 아이돌챔프 클립 1M , 2M를 TB_CLIP_INFO에 전시
                                    mapper.InsertCLIPINFO(callbackInfo.gid, callbackInfo.cid, typeid, hls_url);
                                }

                                // callback_cnt 가 2이상이면 xml ready
                                DataSet ds_cnt = new DataSet();
                                m_sql = String.Format(@"SELECT
                                                        TB_CLIP.callback_cnt
                                                        FROM TB_CLIP
                                                        WHERE 1=1
                                                        AND cid = '{0}'
                                                        LIMIT 0,1", callbackInfo.cid);
                                using (MySqlConnection conn = new MySqlConnection(Singleton.getInstance().GetStrConn()))
                                {
                                    conn.Open();
                                    MySqlDataAdapter adpt_cnt = new MySqlDataAdapter(m_sql, conn);

                                    adpt_cnt.Fill(ds_cnt, "CALLBACK_CNT");
                                }

                                foreach (DataRow r_cnt in ds_cnt.Tables[0].Rows)
                                {
                                    String callback_cnt = r_cnt["callback_cnt"].ToString();
                                    int n_callback = 0;
                                    n_callback = Int32.Parse(callback_cnt);
                                    frmMain.WriteLogThread(String.Format(@"[CDNService] Callback cnt : {0}, cid : {1}", callback_cnt, callbackInfo.cid));
                                    log.logging(String.Format(@"Callback cnt : {0}, cid : {1}", callback_cnt, callbackInfo.cid));

                                    if (n_callback % 2 == 0 || (callbackInfo.transcode_YN == "N" && n_callback >= 1))
                                    {
                                        //callback을 받았으므로 Completed로 변경함
                                        //mapper.UpdateClipCompleted(m_clip_pk);
                                        mapper.UpdateClipStatus(callbackInfo.cid, "Completed");
                                        try
                                        {
                                            DataSet ds_ftp = new DataSet();
                                            m_sql = String.Format(@"SELECT
                                                            TB_CLIP.ftp_target
                                                            FROM TB_CLIP
                                                            WHERE 1=1
                                                            AND cid = '{0}'
                                                            LIMIT 0,1", callbackInfo.cid);
                                            using (MySqlConnection conn = new MySqlConnection(Singleton.getInstance().GetStrConn()))
                                            {
                                                conn.Open();
                                                MySqlDataAdapter adpt_ftp = new MySqlDataAdapter(m_sql, conn);

                                                adpt_ftp.Fill(ds_ftp, "FTP_TARGET");
                                            }

                                            //ds.Tables[0].Rows[0]["c"].ToString();

                                            String[] ftp_target;
                                            foreach (DataRow r_ftp in ds_ftp.Tables[0].Rows)
                                            {
                                                Dictionary<String, Object> map = new Dictionary<String, Object>();

                                                map = r_ftp.Table.Columns
                                                    .Cast<DataColumn>()
                                                    .ToDictionary(col => col.ColumnName, col => r_ftp.Field<Object>(col.ColumnName));

                                                ftp_target = map["ftp_target"].ToString().Split(',');

                                                // 준비된 XML을 각 고객사에 전송
                                                frmMain.WriteLogThread("[CDNService] Ready to send : " + callbackInfo.cid + " target : " + map["ftp_target"].ToString());
                                                if (!String.IsNullOrEmpty(map["ftp_target"].ToString()))
                                                {
                                                    foreach (String customer in ftp_target)
                                                    {
                                                        if (customer != "1" && customer != "2")
                                                        {
                                                            //frmMain.WriteLogThread("[ClipService] customer : " + customer);

                                                            m_sql = String.Format(@"UPDATE
                                                                        TB_FTP_QUEUE SET status = 'Pending'
                                                                        WHERE clip_pk = '{0}'
                                                                        AND customer_id = '{1}'
                                                                        AND type = 'XML'", callbackInfo.clip_pk, customer);
                                                            using (MySqlConnection conn = new MySqlConnection(Singleton.getInstance().GetStrConn()))
                                                            {
                                                                conn.Open();
                                                                cmd = new MySqlCommand(m_sql, conn);
                                                                cmd.ExecuteNonQuery();
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                            ds_ftp.Clear();
                                        }
                                        catch (Exception e)
                                        {
                                            frmMain.WriteLogThread(e.ToString());
                                            log.logging(e.ToString());
                                        }
                                    }
                                }
                                ds_cnt.Clear();
                            }
                        }
                    }
                }
                catch (Exception e)
                {
                    frmMain.WriteLogThread(e.ToString());
                    log.logging(e.ToString());
                }
                ds.Clear();
                Thread.Sleep(1000);
            }

            log.logging("Thread Terminate");
        }
    }
}