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

namespace MBCPLUS_DAEMON
{
    class CopyClipService
    {        
        private Boolean _shouldStop = false;                
        private String m_sql = "";       
        private ConnectionPool connPool;
        private Log log;
        private SqlMapper mapper;

        public CopyClipService()
        {
            //put this className
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

        void Run()
        {
            DataSet ds = new DataSet();            
            String status = null;
            String type = null;
            MySqlCommand cmd;

            String srcpath;
            String dstpath;
            String pk;
            String clip_pk;
            String gid = null;
            String cid = null;
            String m_ftp_target;
            String starttime;
            String endtime;
            String yt_videoid;
            String dm_videoid;
            String edit_count_tail = "";

            connPool = new ConnectionPool();
            connPool.SetConnection(new MySqlConnection(Singleton.getInstance().GetStrConn()));
            
            //Waiting for make winform
            Thread.Sleep(10000);
            //frmMain.WriteLogThread("Copy Clip Service Start...");
            log.logging("Service Start...");
            while (!_shouldStop)
            {
                try
                {
                    mapper.GetCopyClipService(ds);

                    foreach (DataRow r in ds.Tables[0].Rows)
                    {
                        try
                        {
                            pk = r["archive_pk"].ToString();
                            clip_pk = r["clip_pk"].ToString();
                            //m_customer_pk = r["customer_pk"].ToString();
                            starttime = r["starttime"].ToString();
                            endtime = r["endtime"].ToString();
                            srcpath = r["srcpath"].ToString();
                            dstpath = r["dstpath"].ToString();
                            gid = r["gid"].ToString();
                            cid = r["cid"].ToString();
                            status = r["status"].ToString();
                            m_ftp_target = r["ftp_target"].ToString();
                            type = r["type"].ToString();
                            yt_videoid = r["yt_videoid"].ToString();
                            dm_videoid = r["dm_videoid"].ToString();
                            edit_count_tail = r["edit_count_tail"].ToString();


                            connPool.ConnectionOpen();
                            m_sql = String.Format("UPDATE TB_ARCHIVE SET starttime = CURRENT_TIMESTAMP(), status = 'Running' WHERE archive_pk = '{0}'", pk);
                            //Running 으로 변경
                            cmd = new MySqlCommand(m_sql, connPool.getConnection());
                            cmd.ExecuteNonQuery();
                            connPool.ConnectionClose();

                            frmMain.WriteLogThread(String.Format(@"[CopyClipService] cid({0}) is Running", cid));

                            if (!String.IsNullOrEmpty(srcpath) && !String.IsNullOrEmpty(dstpath))
                            {
                                if (File.Exists(dstpath))
                                {
                                    if (srcpath != dstpath)
                                    {
                                        File.Delete(dstpath);
                                        log.logging(String.Format("{0} is deleted", dstpath));
                                    }
                                }
                                //지금부터 COPY 시작
                                CustomFileCopier copier = new CustomFileCopier(srcpath, dstpath);
                                copier.Copy();

                                //Completed 로 변경
                                connPool.ConnectionOpen();
                                m_sql = String.Format(@"UPDATE TB_ARCHIVE SET endtime = CURRENT_TIMESTAMP(), status = 'Completed' WHERE archive_pk = '{0}'", pk);
                                cmd = new MySqlCommand(m_sql, connPool.getConnection());
                                cmd.ExecuteNonQuery();
                                connPool.ConnectionClose();

                                String convert_targetpath = dstpath.Substring(2, dstpath.Length - 2).Replace('\\', '/');
                                frmMain.WriteLogThread(convert_targetpath);

                                if (type.ToLower() == "mov")
                                {
                                    connPool.ConnectionOpen();
                                    m_sql = String.Format(@"UPDATE TB_CLIP SET archive_clip = '{0}' WHERE clip_pk = '{1}'", convert_targetpath, clip_pk);
                                    cmd = new MySqlCommand(m_sql, connPool.getConnection());
                                    cmd.ExecuteNonQuery();
                                    connPool.ConnectionClose();
                                }                                
                                else if (type.ToLower() == "img")
                                {
                                    connPool.ConnectionOpen();
                                    m_sql = String.Format(@"UPDATE TB_CLIP SET archive_img = '{0}' WHERE clip_pk = '{1}'", convert_targetpath, clip_pk);
                                    cmd = new MySqlCommand(m_sql, connPool.getConnection());
                                    cmd.ExecuteNonQuery();
                                    connPool.ConnectionClose();

                                    if (!String.IsNullOrEmpty(dm_videoid))
                                    {
                                        mapper.UpdateDailymotionReady(cid);
                                    }
                                    if (!String.IsNullOrEmpty(yt_videoid))
                                    {
                                        mapper.UpdateYoutubeReady(cid);
                                    }
                                } else if (type.ToLower() == "srt")
                                {
                                    connPool.ConnectionOpen();
                                    m_sql = String.Format(@"UPDATE TB_CLIP SET archive_subtitle = '{0}' WHERE clip_pk = '{1}'", convert_targetpath, clip_pk);
                                    cmd = new MySqlCommand(m_sql, connPool.getConnection());
                                    cmd.ExecuteNonQuery();
                                    connPool.ConnectionClose();
                                } else if ( type.ToLower() == "yt_img")
                                {
                                    connPool.ConnectionOpen();
                                    m_sql = String.Format(@"UPDATE TB_YOUTUBE SET archive_img = '{0}' WHERE cid = '{1}'", convert_targetpath, cid);
                                    cmd = new MySqlCommand(m_sql, connPool.getConnection());
                                    cmd.ExecuteNonQuery();
                                    connPool.ConnectionClose();
                                } else if ( type.ToLower() == "yt_srt")
                                {
                                    connPool.ConnectionOpen();
                                    m_sql = String.Format(@"UPDATE TB_YOUTUBE SET archive_srt = '{0}' WHERE cid = '{1}'", convert_targetpath, cid);
                                    cmd = new MySqlCommand(m_sql, connPool.getConnection());
                                    cmd.ExecuteNonQuery();
                                    connPool.ConnectionClose();
                                } else if (type.ToLower() == "yt_srt_ko")
                                {
                                    mapper.UpdateClipArchivePath(convert_targetpath, cid, "ko");
                                    /*
                                    connPool.ConnectionOpen();
                                    m_sql = String.Format(@"UPDATE TB_YT_ITEMS SET archive_path = '{0}' WHERE cid = '{1}' AND language = 'ko'", convert_targetpath, cid);
                                    cmd = new MySqlCommand(m_sql, connPool.getConnection());
                                    cmd.ExecuteNonQuery();
                                    connPool.ConnectionClose();
                                    */
                                } else if (type.ToLower() == "yt_srt_ja")
                                {
                                    mapper.UpdateClipArchivePath(convert_targetpath, cid, "ja");
                                    /*
                                    connPool.ConnectionOpen();
                                    m_sql = String.Format(@"UPDATE TB_YT_ITEMS SET archive_path = '{0}' WHERE cid = '{1}' AND language = 'ja'", convert_targetpath, cid);
                                    cmd = new MySqlCommand(m_sql, connPool.getConnection());
                                    cmd.ExecuteNonQuery();
                                    connPool.ConnectionClose();
                                    */
                                } else if (type.ToLower() == "yt_srt_en")
                                {
                                    mapper.UpdateClipArchivePath(convert_targetpath, cid, "en");
                                    /*
                                    connPool.ConnectionOpen();
                                    m_sql = String.Format(@"UPDATE TB_YT_ITEMS SET archive_path = '{0}' WHERE cid = '{1}' AND language = 'en'", convert_targetpath, cid);
                                    cmd = new MySqlCommand(m_sql, connPool.getConnection());
                                    cmd.ExecuteNonQuery();
                                    connPool.ConnectionClose();
                                    */
                                } else if (type.ToLower() == "yt_srt_zh")
                                {
                                    mapper.UpdateClipArchivePath(convert_targetpath, cid, "zh");
                                    /*
                                    connPool.ConnectionOpen();
                                    m_sql = String.Format(@"UPDATE TB_YT_ITEMS SET archive_path = '{0}' WHERE cid = '{1}' AND language = 'en'", convert_targetpath, cid);
                                    cmd = new MySqlCommand(m_sql, connPool.getConnection());
                                    cmd.ExecuteNonQuery();
                                    connPool.ConnectionClose();
                                    */
                                }

                                String ftptargetpath = "";
                                ftptargetpath = System.IO.Path.GetDirectoryName(dstpath);
                                ftptargetpath = ftptargetpath.Replace(@"Z:\", "");                                    
                                ftptargetpath = ftptargetpath.Replace(@"\", "/");
                                frmMain.WriteLogThread(String.Format(@"[CopyClipService] {0} ftptargetpath {1}", cid, ftptargetpath));
                                frmMain.WriteLogThread(String.Format(@"[CopyClipService] {0} ftp_target : {1}", cid, m_ftp_target));

                                log.logging(String.Format("{0} ftp_target : {1}", cid, m_ftp_target));
                                if (!String.IsNullOrEmpty(m_ftp_target))
                                {
                                    // Clip상태를 Sending 로 변경                                    
                                    mapper.UpdateClipStatus(cid, "Sending");
                                    
                                    String[] ftp_target = m_ftp_target.Split(',');
                                    foreach (String customer in ftp_target)
                                    {
                                        log.logging(String.Format("{0} customer_id : {1} ({2})", cid, customer, type));
                                        //고객사가 CDN이 아니면 type을 TRANSFER로 변환
                                        String tailName = "";
                                        if ( type.ToLower() == "yt_img" || type.ToLower() == "yt_srt")
                                        {
                                            tailName = "_YT1";
                                        }

                                        if ( type.ToLower() == "yt_srt_ko")
                                        {
                                            tailName = "_YT_SRT_KO";
                                        }

                                        if (type.ToLower() == "yt_srt_ja")
                                        {
                                            tailName = "_YT_SRT_JA";
                                        }

                                        if (type.ToLower() == "yt_srt_en")
                                        {
                                            tailName = "_YT_SRT_EN";
                                        }

                                        if (type.ToLower() == "yt_srt_zh")
                                        {
                                            tailName = "_YT_SRT_ZH";
                                        }

                                        String ftpDestFileName = "";

                                        if(String.IsNullOrEmpty(edit_count_tail))
                                        {
                                            ftpDestFileName = String.Format("{0}{1}{2}", cid.ToUpper(), tailName, Path.GetExtension(dstpath).ToLower());
                                        } else
                                        {
                                            ftpDestFileName = String.Format("{0}{1}{2}{3}", cid.ToUpper(), tailName, edit_count_tail, Path.GetExtension(dstpath).ToLower());
                                        }
                                        
                                        String type_temp = null;
                                        if (customer != "1" || customer != "2")
                                        {
                                            //BBMC나 LG_CDN은 xml을 FTP 대기열에 올리지 않는다
                                            if (type.ToLower() == "mov")
                                            {
                                                // mov면 xml을 대기시킴(생성전임)
                                                connPool.ConnectionOpen();
                                                type_temp = type;
                                                type = "XML";
                                                m_sql = String.Format(@"INSERT INTO TB_FTP_QUEUE (starttime, archive_pk, clip_pk, srcpath, targetfilename, status, type, customer_id, targetpath, gid, cid)
                                                VALUES( CURRENT_TIMESTAMP(), '{0}', '{1}', '{2}', '{3}', 'Ready', '{4}', '{6}', '{5}', '{7}', '{8}')", pk, clip_pk, Util.escapedPath(Path.GetDirectoryName(dstpath) + @"\" + cid + ".xml"), Path.GetFileNameWithoutExtension(ftpDestFileName) + ".xml", type, ftptargetpath, customer, gid, cid);
                                                cmd = new MySqlCommand(m_sql, connPool.getConnection());
                                                cmd.ExecuteNonQuery();
                                                connPool.ConnectionClose();
                                            }
                                        }
                                        //FTP_QUEUE 등록
                                        //String uuidFileName = Guid.NewGuid().ToString().ToUpper() + Path.GetExtension(m_dstpath);                                                                
                                        if (!String.IsNullOrEmpty(type_temp))
                                        {
                                            type = type_temp;
                                        }
                                        
                                        // BBMC 는 img를 보내면 안됨 2018-01-11
                                        if (!(customer == "1" && (type.ToLower() == "img" || Path.GetExtension(dstpath).ToLower() == ".srt" || Path.GetExtension(dstpath).ToLower() == ".xml" )) )
                                        {
                                            log.logging(String.Format("cid : {0}, yt_videoid : {1}", cid, yt_videoid));
                                            connPool.ConnectionOpen();
                                            m_sql = String.Format(@"INSERT INTO TB_FTP_QUEUE (starttime, archive_pk, clip_pk, srcpath, targetfilename, status, type, customer_id, targetpath, gid, cid)
                                            VALUES( CURRENT_TIMESTAMP(), '{0}', '{1}', '{2}', '{3}', 'Pending', '{4}', '{6}', '{5}', '{7}', '{8}')",
                                            pk,
                                            clip_pk,
                                            Util.escapedPath(dstpath), //srcpath
                                            ftpDestFileName, //targetfilename
                                            type, //type
                                            ftptargetpath, //targetpath
                                            customer,
                                            gid,
                                            cid);
                                            cmd = new MySqlCommand(m_sql, connPool.getConnection());
                                            cmd.ExecuteNonQuery();
                                            connPool.ConnectionClose();
                                        }
                                    }
                                }
                                else
                                {                                    
                                    mapper.UpdateClipStatus(cid, "Completed");
                                }
                            }
                            else
                            {
                                //copy 실패
                                log.logging(String.Format("Copy failed ({0} -> {1}", srcpath, dstpath));
                            }                            
                        }
                        catch (Exception ex)
                        {
                            mapper.UpdateClipStatus(cid, "Failed", ex.ToString());
                            //frmMain.WriteLogThread("[CopyClipService] " + ex.ToString());
                            log.logging(ex.ToString());
                        }
                    }
                }
                catch (Exception e)
                {
                    //frmMain.WriteLogThread(e.ToString());
                    log.logging(e.ToString());
                }
                Thread.Sleep(1000);
                ds.Clear();
            }
            connPool.ConnectionDisPose();
            log.logging("Thread Terminate");
        }
    }
}
