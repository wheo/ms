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
        private String m_srcpath;
        private String m_dstpath;        
        private String m_pk;
        private String m_clip_pk;
        private String m_gid;
        private String m_cid;        
        private String m_ftp_target;
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
            connPool = new ConnectionPool();
            connPool.SetConnection(new MySqlConnection(Singleton.getInstance().GetStrConn()));
            
            //Waiting for make winform
            Thread.Sleep(5000);
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
                            m_pk = r["archive_pk"].ToString();
                            m_clip_pk = r["clip_pk"].ToString();
                            //m_customer_pk = r["customer_pk"].ToString();
                            m_srcpath = r["srcpath"].ToString();
                            m_dstpath = r["dstpath"].ToString();
                            m_gid = r["gid"].ToString();
                            m_cid = r["cid"].ToString();
                            status = r["status"].ToString();
                            m_ftp_target = r["ftp_target"].ToString();
                            type = r["type"].ToString();
                            
                            connPool.ConnectionOpen();
                            m_sql = String.Format("UPDATE TB_ARCHIVE SET starttime = CURRENT_TIMESTAMP(), status = 'Running' WHERE archive_pk = '{0}'", m_pk);
                            //Running 으로 변경
                            cmd = new MySqlCommand(m_sql, connPool.getConnection());
                            cmd.ExecuteNonQuery();
                            connPool.ConnectionClose();

                            frmMain.WriteLogThread(String.Format(@"[CopyService] archive_pk({0}) is Running", m_pk));

                            if (!String.IsNullOrEmpty(m_srcpath) && !String.IsNullOrEmpty(m_dstpath))
                            {
                                if (File.Exists(m_dstpath))
                                {
                                    File.Delete(m_dstpath);
                                }
                                //지금부터 COPY 시작
                                CustomFileCopier copier = new CustomFileCopier(m_srcpath, m_dstpath);
                                copier.Copy();

                                //Completed 로 변경
                                connPool.ConnectionOpen();
                                m_sql = String.Format(@"UPDATE TB_ARCHIVE SET endtime = CURRENT_TIMESTAMP(), status = 'Completed' WHERE archive_pk = '{0}'", m_pk);
                                cmd = new MySqlCommand(m_sql, connPool.getConnection());
                                cmd.ExecuteNonQuery();
                                connPool.ConnectionClose();

                                String convert_targetpath = m_dstpath.Substring(2, m_dstpath.Length - 2).Replace('\\', '/');
                                frmMain.WriteLogThread(convert_targetpath);

                                if (type.ToLower() == "mov")
                                {
                                    connPool.ConnectionOpen();
                                    m_sql = String.Format(@"UPDATE TB_CLIP SET archive_clip = '{0}' WHERE clip_pk = '{1}'", convert_targetpath, m_clip_pk);
                                    cmd = new MySqlCommand(m_sql, connPool.getConnection());
                                    cmd.ExecuteNonQuery();
                                    connPool.ConnectionClose();
                                }
                                else if (type.ToLower() == "img")
                                {
                                    connPool.ConnectionOpen();
                                    m_sql = String.Format(@"UPDATE TB_CLIP SET archive_img = '{0}' WHERE clip_pk = '{1}'", convert_targetpath, m_clip_pk);
                                    cmd = new MySqlCommand(m_sql, connPool.getConnection());
                                    cmd.ExecuteNonQuery();
                                    connPool.ConnectionClose();
                                }

                                // 원본파일을 삭제        
                                //File.Delete(m_srcpath);
                                //frmMain.WriteLogThread(String.Format(@"[CopyService] {0} is deleted", m_srcpath));

                                String ftptargetpath = "";
                                ftptargetpath = System.IO.Path.GetDirectoryName(m_dstpath);
                                ftptargetpath = ftptargetpath.Replace(@"Z:\", "");                                    
                                ftptargetpath = ftptargetpath.Replace(@"\", "/");
                                frmMain.WriteLogThread(String.Format(@"[CopyService] {0}", ftptargetpath));

                                if (!String.IsNullOrEmpty(m_ftp_target))
                                {
                                    // Clip상태를 Sending 로 변경
                                    connPool.ConnectionOpen();
                                    m_sql = String.Format(@"UPDATE TB_CLIP SET status = 'Sending' WHERE clip_pk = '{0}'", m_clip_pk);
                                    cmd = new MySqlCommand(m_sql, connPool.getConnection());
                                    cmd.ExecuteNonQuery();
                                    connPool.ConnectionClose();

                                    String[] ftp_target = m_ftp_target.Split(',');
                                    foreach (String customer in ftp_target)
                                    {
                                        //고객사가 CDN이 아니면 type을 TRANSFER로 변환
                                        String cidFileName = m_cid.ToUpper() + Path.GetExtension(m_dstpath).ToLower();
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
                                                VALUES( CURRENT_TIMESTAMP(), '{0}', '{1}', '{2}', '{3}', 'Ready', '{4}', '{6}', '{5}', '{7}', '{8}')", m_pk, m_clip_pk, Util.escapedPath(Path.GetDirectoryName(m_dstpath) + @"\" + m_cid + ".xml"), Path.GetFileNameWithoutExtension(cidFileName) + ".xml", type, ftptargetpath, customer, m_gid, m_cid);
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
                                        if (!(customer == "1" && type.ToLower() == "img"))
                                        {
                                            connPool.ConnectionOpen();
                                            m_sql = String.Format(@"INSERT INTO TB_FTP_QUEUE (starttime, archive_pk, clip_pk, srcpath, targetfilename, status, type, customer_id, targetpath, gid, cid)
                                                VALUES( CURRENT_TIMESTAMP(), '{0}', '{1}', '{2}', '{3}', 'Pending', '{4}', '{6}', '{5}', '{7}', '{8}')", m_pk, m_clip_pk, Util.escapedPath(m_dstpath), cidFileName, type, ftptargetpath, customer, m_gid, m_cid);
                                            cmd = new MySqlCommand(m_sql, connPool.getConnection());
                                            cmd.ExecuteNonQuery();
                                            connPool.ConnectionClose();
                                        }
                                    }
                                }
                                else
                                {
                                    // 보낼 곳이 없으므로 Clip상태를 Ready 로 변경
                                    connPool.ConnectionOpen();
                                    m_sql = String.Format(@"UPDATE TB_CLIP SET status = 'Ready' WHERE clip_pk = '{0}'", m_clip_pk);
                                    cmd = new MySqlCommand(m_sql, connPool.getConnection());
                                    cmd.ExecuteNonQuery();
                                    connPool.ConnectionClose();
                                }
                            }
                            else
                            {
                                //copy 실패
                            }                            
                        }
                        catch (Exception ex)
                        {
                            frmMain.WriteLogThread("[CopyClipService] " + ex.ToString());
                            log.logging(ex.ToString());
                        }
                    }
                }
                catch (Exception e)
                {
                    frmMain.WriteLogThread(e.ToString());
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
