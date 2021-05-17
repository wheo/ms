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
    internal class CopySmrProgramService
    {
        private Boolean _shouldStop = false;

        private SqlMapper mapper;

        private Log log;

        public CopySmrProgramService()
        {
            //put this className
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

        private void Run()
        {
            DataSet ds = new DataSet();
            String status = null;
            String type = null;

            String srcpath;
            String dstpath;
            String pk;
            String pid;
            String img_type;
            String edit_count_tail = "";

            //Waiting for make winform
            Thread.Sleep(5000);
            //frmMain.WriteLogThread("Copying Program Service Start...");
            log.logging("Service Start...");
            while (!_shouldStop)
            {
                try
                {
                    mapper.GetCopySmrProgramService(ds);
                    foreach (DataRow r in ds.Tables[0].Rows)
                    {
                        pk = r["pk"].ToString();
                        pid = r["pid"].ToString();
                        //m_customer_pk = r["customer_pk"].ToString();
                        srcpath = r["srcpath"].ToString();
                        dstpath = r["dstpath"].ToString();
                        status = r["status"].ToString();
                        type = r["type"].ToString();
                        img_type = r["smr_img_type"].ToString();
                        edit_count_tail = r["edit_count_tail"].ToString();

                        using (MySqlConnection conn = new MySqlConnection(Singleton.getInstance().GetStrConn()))
                        {
                            conn.Open();
                            string sql = String.Format("UPDATE TB_ARCHIVE SET starttime = CURRENT_TIMESTAMP(), status = 'Running' WHERE archive_pk = '{0}'", pk);
                            //Running 으로 변경
                            MySqlCommand cmd = new MySqlCommand(sql, conn);
                            cmd.ExecuteNonQuery();
                        }

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

                            string dstpath_b = dstpath.Replace("Z:", "W:");
                            CustomFileCopier copier_b = new CustomFileCopier(srcpath, dstpath_b);
                            copier_b.Copy();

                            //Completed 로 변경
                            using (MySqlConnection conn = new MySqlConnection(Singleton.getInstance().GetStrConn()))
                            {
                                conn.Open();
                                string sql = String.Format(@"UPDATE TB_ARCHIVE SET endtime = CURRENT_TIMESTAMP(), status = 'Completed' WHERE archive_pk = '{0}'", pk);
                                MySqlCommand cmd = new MySqlCommand(sql, conn);
                                cmd.ExecuteNonQuery();
                            }

                            //Z:\mbcplus\archive\sports\2016\10\31\GA201610310001\CA201610310001\160824_0824_모비스vs동부_C_05_2쿼터_모처럼_속공_성공시키는_전준범.mp4
                            String convert_targetpath = dstpath.Substring(2, dstpath.Length - 2).Replace('\\', '/');
                            String img_type_name = "";
                            String tail = "";

                            log.logging(convert_targetpath);

                            if (img_type == "1")
                            {
                                img_type_name = "archive_img";
                                tail = "";
                            }
                            else if (img_type == "2")
                            {
                                img_type_name = "archive_posterimg1";
                                tail = "_P1";
                            }
                            else if (img_type == "3")
                            {
                                img_type_name = "archive_posterimg2";
                                tail = "_P2";
                            }
                            else if (img_type == "4")
                            {
                                img_type_name = "archive_bannerimg";
                                tail = "_B";
                            }
                            else if (img_type == "5")
                            {
                                img_type_name = "archive_thumbimg";
                                tail = "_T";
                            }
                            if (!String.IsNullOrEmpty(img_type_name))
                            {
                                using (MySqlConnection conn = new MySqlConnection(Singleton.getInstance().GetStrConn()))
                                {
                                    conn.Open();
                                    string sql = String.Format(@"UPDATE TB_SMR_PROGRAM SET {2} = '{0}' WHERE pid = '{1}'", convert_targetpath, pid, img_type_name);
                                    MySqlCommand cmd = new MySqlCommand(sql, conn);
                                    cmd.ExecuteNonQuery();
                                }
                            }
                            else
                            {
                                log.logging(String.Format("img_type_name is null({0})", img_type));
                            }

                            // 원본 파일을 삭제
                            //File.Delete(m_srcpath);
                            //frmMain.WriteLogThread(String.Format(@"[CopyProgramService] {0} is deleted", m_srcpath));

                            //SEQ 상태를 Sending 로 변경
                            using (MySqlConnection conn = new MySqlConnection(Singleton.getInstance().GetStrConn()))
                            {
                                conn.Open();
                                string sql = String.Format(@"UPDATE TB_SMR_PROGRAM SET status = 'Sending' WHERE pid = '{0}'", pid);
                                MySqlCommand cmd = new MySqlCommand(sql, conn);
                                cmd.ExecuteNonQuery();
                            }

                            String ftptargetpath = "";
                            ftptargetpath = System.IO.Path.GetDirectoryName(dstpath);
                            ftptargetpath = ftptargetpath.Replace(@"Z:\", "");
                            ftptargetpath = ftptargetpath.Replace(@"\", "/");
                            frmMain.WriteLogThread(ftptargetpath);

                            //FTP_QUEUE 등록
                            //String uuidFileName = Guid.NewGuid().ToString().ToUpper() + Path.GetExtension(m_dstpath);
                            String FileName = "";
                            if (String.IsNullOrEmpty(edit_count_tail))
                            {
                                FileName = String.Format("{0}{1}{2}", pid.ToUpper(), tail, Path.GetExtension(dstpath));
                            }
                            else
                            {
                                FileName = String.Format("{0}{1}{2}{3}", pid.ToUpper(), tail, edit_count_tail, Path.GetExtension(dstpath));
                            }
                            using (MySqlConnection conn = new MySqlConnection(Singleton.getInstance().GetStrConn()))
                            {
                                conn.Open();
                                string sql = String.Format(@"INSERT INTO TB_FTP_QUEUE (starttime, archive_pk, smr_pid, srcpath, targetfilename, status, type, customer_id, targetpath, smr_img_type)
                                                VALUES( CURRENT_TIMESTAMP(), '{0}', '{1}', '{2}', '{3}', 'Pending', '{4}', '2', '{5}', {6})", pk
                                                , pid
                                                , Util.escapedPath(dstpath)
                                                , FileName
                                                , type
                                                , ftptargetpath
                                                , img_type);
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