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
    class ArchiveProgramSeqService
    {        
        private Boolean _shouldStop = false;        
        private String m_imgsrcpath;
        //private String m_clipsrcpath;
        //private String m_dstpath;        
        private String m_orgimgname;        
        private String m_pk;
        private String m_gid;
        private String m_cdnurl_img;
        private String m_archive_date;
        //private String m_cid;
        private String m_section;
        private String m_sql = "";
        private ConnectionPool connPool;
        private Log log;
        private SqlMapper mapper;

        public ArchiveProgramSeqService()
        {
            // put class name
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
            String status = null;
            MySqlCommand cmd;

            connPool = new ConnectionPool();
            connPool.SetConnection(new MySqlConnection(Singleton.getInstance().GetStrConn()));            
            
            //Waiting for make winform
            Thread.Sleep(5000);
            //frmMain.WriteLogThread("Archive Program Service Start...");
            log.logging("Service Start...");

            while (!_shouldStop)
            {
                try
                {
                    DataSet ds = new DataSet();
                    mapper.GetArchiveProgramServiceInfo(ds);                    

                    foreach (DataRow r in ds.Tables[0].Rows)
                    {
                        try
                        {
                            m_pk = r["program_seq_pk"].ToString();
                            m_imgsrcpath = r["imgsrcpath"].ToString();
                            m_orgimgname = r["orgimgname"].ToString();
                            m_gid = r["gid"].ToString();
                            m_cdnurl_img = r["cdnurl_img"].ToString();
                            m_archive_date = r["archive_date"].ToString();
                            status = r["status"].ToString();
                            m_section = r["section"].ToString();
                            
                            if (!String.IsNullOrEmpty(m_imgsrcpath))
                            {
                                mapper.UpdateArchiveServiceRunning(m_pk);

                                frmMain.WriteLogThread(String.Format(@"program_seq_pk({0}) is Archive", m_pk));
                                String targetPath = "";

                                // 스포츠, 예능 구분해야함(프로그램 정보로부터 가져올 수 있음)
                                StringBuilder sb = new StringBuilder();
                                sb.Append(Util.getSectionPath(m_section));
                                sb.Append(m_archive_date);
                                sb.Append(Path.DirectorySeparatorChar);
                                sb.Append(m_gid);

                                targetPath = sb.ToString();

                                frmMain.WriteLogThread(targetPath);

                                if (!String.IsNullOrEmpty(m_imgsrcpath))
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
                                    m_sql = String.Format(@"INSERT INTO TB_ARCHIVE(insert_time, program_seq_pk, srcpath, targetpath, type, status)
                                                VALUES (CURRENT_TIMESTAMP(), '{0}', '{1}', '{2}', 'IMG', 'Pending')", m_pk, Util.escapedPath(m_imgsrcpath), Util.escapedPath(targetPath + Path.DirectorySeparatorChar + m_gid + Path.GetExtension(m_orgimgname.ToLower())));

                                    cmd = new MySqlCommand(m_sql, connPool.getConnection());
                                    cmd.ExecuteNonQuery();
                                    connPool.ConnectionClose();
                                }
                            }
                            else if (m_cdnurl_img.Length > 7)
                            {
                                connPool.ConnectionOpen();
                                m_sql = String.Format("UPDATE TB_PROGRAM_SEQ SET starttime = CURRENT_TIMESTAMP(), status = 'Ready' WHERE program_seq_pk = '{0}'", m_pk);                                    
                                cmd = new MySqlCommand(m_sql, connPool.getConnection());
                                cmd.ExecuteNonQuery();
                                connPool.ConnectionClose();
                                frmMain.WriteLogThread(String.Format(@"[ArchiveProgramService] {0} is already exist, clip_pk = {1}", m_cdnurl_img, m_pk));
                            }                            
                        }
                        catch (Exception e)
                        {
                            log.logging(e.ToString());
                            frmMain.WriteLogThread("[ArchveProgramService] " + e.ToString());
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
            log.logging("Thread Terminate" + _shouldStop);
        }
    }
}