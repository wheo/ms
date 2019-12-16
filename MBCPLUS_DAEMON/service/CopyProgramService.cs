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
    class CopyProgramService
    {        
        private Boolean _shouldStop = false;        
        private String m_srcpath;
        private String m_dstpath;        
        private String m_pk;
        private String m_pid;        
        private String m_sql = "";       
        private ConnectionPool connPool;
        private SqlMapper mapper;

        private Log log;

        public CopyProgramService()
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
            //frmMain.WriteLogThread("Copying Program Service Start...");
            log.logging("Service Start...");
            while (!_shouldStop)
            {
                try
                {
                    mapper.GetCopyProgramService(ds);                    
                    foreach (DataRow r in ds.Tables[0].Rows)
                    {
                        m_pk = r["pk"].ToString();
                        m_pid = r["pid"].ToString();
                        //m_customer_pk = r["customer_pk"].ToString();
                        m_srcpath = r["srcpath"].ToString();
                        m_dstpath = r["dstpath"].ToString();                        
                        status = r["status"].ToString();
                        type = r["type"].ToString();                        
                        connPool.ConnectionOpen();
                        m_sql = String.Format("UPDATE TB_ARCHIVE SET starttime = CURRENT_TIMESTAMP(), status = 'Running' WHERE archive_pk = '{0}'", m_pk);
                        //Running 으로 변경
                        cmd = new MySqlCommand(m_sql, connPool.getConnection());
                        cmd.ExecuteNonQuery();
                        connPool.ConnectionClose();

                        frmMain.WriteLogThread(String.Format(@"archive_pk({0}) is Running", m_pk));
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

                            //Z:\mbcplus\archive\sports\2016\10\31\GA201610310001\CA201610310001\160824_0824_모비스vs동부_C_05_2쿼터_모처럼_속공_성공시키는_전준범.mp4
                            String convert_targetpath = m_dstpath.Substring(2, m_dstpath.Length - 2).Replace('\\', '/');

                            frmMain.WriteLogThread(convert_targetpath);
                            connPool.ConnectionOpen();
                            m_sql = String.Format(@"UPDATE TB_PROGRAM SET archive_img = '{0}' WHERE pid = '{1}'", convert_targetpath, m_pid);
                            cmd = new MySqlCommand(m_sql, connPool.getConnection());
                            cmd.ExecuteNonQuery();
                            connPool.ConnectionClose();

                            // 원본 파일을 삭제
                            //File.Delete(m_srcpath);
                            //frmMain.WriteLogThread(String.Format(@"[CopyProgramService] {0} is deleted", m_srcpath));

                            //SEQ 상태를 Sending 로 변경
                            connPool.ConnectionOpen();
                            m_sql = String.Format(@"UPDATE TB_PROGRAM SET status = 'Sending' WHERE pid = '{0}'", m_pid);
                            cmd = new MySqlCommand(m_sql, connPool.getConnection());
                            cmd.ExecuteNonQuery();
                            connPool.ConnectionClose();

                            String ftptargetpath = "";
                            ftptargetpath = System.IO.Path.GetDirectoryName(m_dstpath);
                            ftptargetpath = ftptargetpath.Replace(@"Z:\", "");
                            ftptargetpath = ftptargetpath.Replace(@"\", "/");
                            frmMain.WriteLogThread(ftptargetpath);

                            //FTP_QUEUE 등록
                            //String uuidFileName = Guid.NewGuid().ToString().ToUpper() + Path.GetExtension(m_dstpath);
                            String FileName = m_pid.ToUpper() + Path.GetExtension(m_dstpath);
                            connPool.ConnectionOpen();
                            m_sql = String.Format(@"INSERT INTO TB_FTP_QUEUE (starttime, archive_pk, pid, srcpath, targetfilename, status, type, customer_id, targetpath)
                                                VALUES( CURRENT_TIMESTAMP(), '{0}', '{1}', '{2}', '{3}', 'Pending', '{4}', '2', '{5}')", m_pk, m_pid, Util.escapedPath(m_dstpath), FileName, type, ftptargetpath);
                            cmd = new MySqlCommand(m_sql, connPool.getConnection());
                            cmd.ExecuteNonQuery();
                            connPool.ConnectionClose();
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