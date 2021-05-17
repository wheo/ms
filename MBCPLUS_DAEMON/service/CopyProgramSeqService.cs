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
    internal class CopyProgramSeqService
    {
        private Boolean _shouldStop = false;
        private String m_edit_count_tail = "";
        private String m_srcpath;
        private String m_dstpath;
        private String m_pk;
        private String m_program_seq_pk;
        private String m_gid;

        private SqlMapper mapper;

        private Log log;

        public CopyProgramSeqService()
        {
            //put this className
            log = new Log(this.GetType().Name);
            DoWork();
        }

        private void DoWork()
        {
            mapper = new SqlMapper();
            Thread t1 = new Thread(new ThreadStart(Run));
            t1.Start();
        }

        public void RequestStop()
        {
            _shouldStop = true;
        }

        private void Run()
        {
            DataSet ds = new DataSet();
            String status = null;
            String type = null;

            //Waiting for make winform
            Thread.Sleep(5000);
            //frmMain.WriteLogThread("Copying Program seq Service Start...");
            log.logging("Service Start...");
            while (!_shouldStop)
            {
                try
                {
                    mapper.GetCopyPrgramSeqInfo(ds);

                    foreach (DataRow r in ds.Tables[0].Rows)
                    {
                        m_pk = r["archive_pk"].ToString();
                        m_program_seq_pk = r["program_seq_pk"].ToString();
                        m_edit_count_tail = r["edit_count_tail"].ToString();
                        //m_customer_pk = r["customer_pk"].ToString();
                        m_srcpath = r["srcpath"].ToString();
                        m_dstpath = r["dstpath"].ToString();
                        m_gid = r["gid"].ToString();
                        status = r["status"].ToString();
                        type = r["type"].ToString();

                        using (MySqlConnection conn = new MySqlConnection(Singleton.getInstance().GetStrConn()))
                        {
                            conn.Open();
                            string sql = String.Format("UPDATE TB_ARCHIVE SET starttime = CURRENT_TIMESTAMP(), status = 'Running' WHERE archive_pk = '{0}'", m_pk);
                            //Running 으로 변경
                            MySqlCommand cmd = new MySqlCommand(sql, conn);
                            cmd.ExecuteNonQuery();
                        }

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

                            string dstpath_b = m_dstpath.Replace("Z:", "W:");
                            CustomFileCopier copier_b = new CustomFileCopier(m_srcpath, dstpath_b);
                            copier_b.Copy();

                            //Completed 로 변경
                            using (MySqlConnection conn = new MySqlConnection(Singleton.getInstance().GetStrConn()))
                            {
                                conn.Open();
                                string sql = String.Format(@"UPDATE TB_ARCHIVE SET endtime = CURRENT_TIMESTAMP(), status = 'Completed' WHERE archive_pk = '{0}'", m_pk);
                                MySqlCommand cmd = new MySqlCommand(sql, conn);
                                cmd.ExecuteNonQuery();
                            }

                            //Z:\mbcplus\archive\sports\2016\10\31\GA201610310001\CA201610310001\160824_0824_모비스vs동부_C_05_2쿼터_모처럼_속공_성공시키는_전준범.mp4
                            String convert_targetpath = m_dstpath.Substring(2, m_dstpath.Length - 2).Replace('\\', '/');

                            String type_name = "";
                            String tail = "";

                            if (type.ToLower() == "img")
                            {
                                type_name = "archive_img";
                                tail = "";
                            }
                            else if (type.ToLower() == "cue")
                            {
                                type_name = "archive_cue";
                                tail = "_CUE";
                            }
                            else if (type.ToLower() == "script")
                            {
                                type_name = "archive_script";
                                tail = "_SCRIPT";
                            }

                            frmMain.WriteLogThread(convert_targetpath);
                            using (MySqlConnection conn = new MySqlConnection(Singleton.getInstance().GetStrConn()))
                            {
                                conn.Open();
                                string sql = String.Format(@"UPDATE TB_PROGRAM_SEQ SET {2} = '{0}' WHERE program_seq_pk = '{1}'", convert_targetpath, m_program_seq_pk, type_name);
                                MySqlCommand cmd = new MySqlCommand(sql, conn);
                                cmd.ExecuteNonQuery();
                            }

                            // 원본 파일을 삭제
                            //File.Delete(m_srcpath);
                            //frmMain.WriteLogThread(String.Format(@"[CopyProgramService] {0} is deleted", m_srcpath));

                            //SEQ 상태를 Sending 로 변경
                            using (MySqlConnection conn = new MySqlConnection(Singleton.getInstance().GetStrConn()))
                            {
                                conn.Open();
                                string sql = String.Format(@"UPDATE TB_PROGRAM_SEQ SET status = 'Sending' WHERE program_seq_pk = '{0}'", m_program_seq_pk);
                                MySqlCommand cmd = new MySqlCommand(sql, conn);
                                cmd.ExecuteNonQuery();
                            }

                            String ftptargetpath = "";
                            ftptargetpath = System.IO.Path.GetDirectoryName(m_dstpath);
                            ftptargetpath = ftptargetpath.Replace(@"Z:\", "");
                            ftptargetpath = ftptargetpath.Replace(@"\", "/");
                            frmMain.WriteLogThread(ftptargetpath);

                            //FTP_QUEUE 등록
                            //String uuidFileName = Guid.NewGuid().ToString().ToUpper() + Path.GetExtension(m_dstpath);

                            String FileName = "";
                            if (String.IsNullOrEmpty(m_edit_count_tail))
                            {
                                FileName = String.Format("{0}{1}{2}", m_gid.ToUpper(), tail, Path.GetExtension(m_dstpath));
                            }
                            else
                            {
                                FileName = String.Format("{0}{1}{2}{3}", m_gid.ToUpper(), tail, m_edit_count_tail, Path.GetExtension(m_dstpath));
                            }
                            // 회차 이미지는 customer_id : 2  LG_CDN으로 보냄
                            using (MySqlConnection conn = new MySqlConnection(Singleton.getInstance().GetStrConn()))
                            {
                                conn.Open();
                                string sql = String.Format(@"INSERT INTO TB_FTP_QUEUE (starttime, archive_pk, program_seq_pk, srcpath, targetfilename, status, type, customer_id, targetpath, gid)
                                                VALUES( CURRENT_TIMESTAMP(), '{0}', '{1}', '{2}', '{3}', 'Pending', '{4}', '{7}', '{5}', '{6}')", m_pk, m_program_seq_pk, Util.escapedPath(m_dstpath), FileName, type, ftptargetpath, m_gid, "2");
                                MySqlCommand cmd = new MySqlCommand(sql, conn);
                                cmd.ExecuteNonQuery();
                            }
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

            log.logging("Thread Terminate");
        }
    }
}