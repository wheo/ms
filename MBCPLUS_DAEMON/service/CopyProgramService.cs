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
            String srcpath;
            String dstpath;
            String pk;
            String pid;
            String img_type;
            String edit_count_tail = "";

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
                        pk = r["pk"].ToString();
                        pid = r["pid"].ToString();
                        edit_count_tail = r["edit_count_tail"].ToString();
                        //m_customer_pk = r["customer_pk"].ToString();
                        srcpath = r["srcpath"].ToString();
                        dstpath = r["dstpath"].ToString();                        
                        status = r["status"].ToString();
                        type = r["type"].ToString();
                        img_type = r["program_img_type"].ToString();

                        connPool.ConnectionOpen();
                        m_sql = String.Format("UPDATE TB_ARCHIVE SET starttime = CURRENT_TIMESTAMP(), status = 'Running' WHERE archive_pk = '{0}'", pk);
                        //Running 으로 변경
                        cmd = new MySqlCommand(m_sql, connPool.getConnection());
                        cmd.ExecuteNonQuery();
                        connPool.ConnectionClose();

                        frmMain.WriteLogThread(String.Format(@"archive_pk({0}) is Running", pk));
                        if (!String.IsNullOrEmpty(srcpath) && !String.IsNullOrEmpty(dstpath))
                        {
                            if (File.Exists(dstpath))
                            {
                                File.Delete(dstpath);
                            }
                            //지금부터 COPY 시작
                            CustomFileCopier copier = new CustomFileCopier(srcpath, dstpath);
                            copier.Copy();

                            //UpdateArchiveStatue 로 수정해야 함(리팩토링 2019-01-28 아직 안함)
                            //Completed 로 변경
                            connPool.ConnectionOpen();
                            m_sql = String.Format(@"UPDATE TB_ARCHIVE SET endtime = CURRENT_TIMESTAMP(), status = 'Completed' WHERE archive_pk = '{0}'", pk);
                            cmd = new MySqlCommand(m_sql, connPool.getConnection());
                            cmd.ExecuteNonQuery();
                            connPool.ConnectionClose();
                            
                            String convert_targetpath = dstpath.Substring(2, dstpath.Length - 2).Replace('\\', '/');

                            String img_type_name = "";
                            String tail = "";

                            if (img_type == "1")
                            {
                                img_type_name = "archive_img";
                                tail = "";
                            }
                            else if (img_type == "2")
                            {
                                img_type_name = "archive_poster_img";
                                tail = "_P";
                            }
                            else if (img_type == "3")
                            {
                                img_type_name = "archive_thumb_img";
                                tail = "_T";
                            }
                            else if (img_type == "4")
                            {
                                img_type_name = "archive_circle_img";
                                tail = "_C";
                            }  
                            else if ( img_type == "5")
                            {
                                img_type_name = "archive_highres_img";
                                tail = "_H";
                            }
                            else if ( img_type == "6")
                            {
                                img_type_name = "archive_logo_img";
                                tail = "_L";
                            }
                            if (!String.IsNullOrEmpty(img_type_name))
                            {
                                connPool.ConnectionOpen();
                                m_sql = String.Format(@"UPDATE TB_PROGRAM SET {2} = '{0}' WHERE pid = '{1}'", convert_targetpath, pid, img_type_name);
                                cmd = new MySqlCommand(m_sql, connPool.getConnection());
                                cmd.ExecuteNonQuery();
                                connPool.ConnectionClose();
                            }
                            else
                            {
                                log.logging(String.Format("img_type_name is null({0})", img_type));
                            }

                            /*
                            frmMain.WriteLogThread(convert_targetpath);
                            connPool.ConnectionOpen();
                            m_sql = String.Format(@"UPDATE TB_PROGRAM SET archive_img = '{0}' WHERE pid = '{1}'", convert_targetpath, m_pid);
                            cmd = new MySqlCommand(m_sql, connPool.getConnection());
                            cmd.ExecuteNonQuery();
                            connPool.ConnectionClose();
                            */

                            // 원본 파일을 삭제
                            //File.Delete(m_srcpath);
                            //frmMain.WriteLogThread(String.Format(@"[CopyProgramService] {0} is deleted", m_srcpath));

                            //SEQ 상태를 Sending 로 변경
                            connPool.ConnectionOpen();
                            m_sql = String.Format(@"UPDATE TB_PROGRAM SET status = 'Sending' WHERE pid = '{0}'", pid);
                            cmd = new MySqlCommand(m_sql, connPool.getConnection());
                            cmd.ExecuteNonQuery();
                            connPool.ConnectionClose();

                            String ftptargetpath = "";
                            ftptargetpath = System.IO.Path.GetDirectoryName(dstpath);
                            ftptargetpath = ftptargetpath.Replace(@"Z:\", "");
                            ftptargetpath = ftptargetpath.Replace(@"\", "/");
                            frmMain.WriteLogThread(ftptargetpath);

                            //FTP_QUEUE 등록
                            //String uuidFileName = Guid.NewGuid().ToString().ToUpper() + Path.GetExtension(m_dstpath);
                            //String FileName = pid.ToUpper() + Path.GetExtension(dstpath);
                            String FileName = "";
                            if (String.IsNullOrEmpty(edit_count_tail))
                            {
                                FileName = String.Format("{0}{1}{2}", pid.ToUpper(), tail, Path.GetExtension(dstpath));
                            } else
                            {
                                FileName = String.Format("{0}{1}{2}{3}", pid.ToUpper(), tail, edit_count_tail, Path.GetExtension(dstpath));
                            }
                            connPool.ConnectionOpen();
                            m_sql = String.Format(@"INSERT INTO TB_FTP_QUEUE (starttime, archive_pk, pid, srcpath, targetfilename, status, type, customer_id, targetpath, program_img_type)
                                                VALUES( CURRENT_TIMESTAMP(), '{0}', '{1}', '{2}', '{3}', 'Pending', '{4}', '2', '{5}', {6})", pk, pid, Util.escapedPath(dstpath), FileName, type, ftptargetpath, img_type);
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