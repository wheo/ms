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
    class ArchiveSmrProgramService
    {
        private Boolean _shouldStop = false;        
        private ConnectionPool connPool;
        private SqlMapper mapper;

        private Log log;

        public ArchiveSmrProgramService()
        {
            // put this className
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
            //MySqlCommand cmd;

            connPool = new ConnectionPool();
            connPool.SetConnection(new MySqlConnection(Singleton.getInstance().GetStrConn()));
            
            //Waiting for make winform
            Thread.Sleep(5000);
            //frmMain.WriteLogThread("Programseq Archive Service Start...");
            log.logging("Service Start...");

            while (!_shouldStop)
            {
                try
                {
                    mapper.GetProgramSmrService(ds);
                    foreach (DataRow r in ds.Tables[0].Rows)
                    {                        
                        //String pid = r["pid"].ToString();
                        // ADD log
                        vo.SmrProgramInfo smrProgramInfo = new vo.SmrProgramInfo();
                        smrProgramInfo.pid = r["pid"].ToString();
                        smrProgramInfo.img = r["img"].ToString();
                        smrProgramInfo.org_img = r["org_img"].ToString();
                        smrProgramInfo.posterimg1 = r["posterimg1"].ToString();
                        smrProgramInfo.posterimg2 = r["posterimg2"].ToString();
                        smrProgramInfo.org_posterimg1 = r["org_posterimg1"].ToString();
                        smrProgramInfo.org_posterimg2 = r["posterimg2"].ToString();
                        smrProgramInfo.org_posterimg2 = r["org_posterimg2"].ToString();
                        smrProgramInfo.bannerimg = r["bannerimg"].ToString();
                        smrProgramInfo.org_bannerimg = r["org_bannerimg"].ToString();
                        smrProgramInfo.thumbimg = r["thumbimg"].ToString();
                        smrProgramInfo.org_thumbimg = r["org_thumbimg"].ToString();             
                        
                        smrProgramInfo.edit_img_count = Convert.ToInt32(r["edit_img_count"].ToString());
                        smrProgramInfo.edit_img_poster1_count = Convert.ToInt32(r["edit_img_poster1_count"].ToString());
                        smrProgramInfo.edit_img_poster2_count = Convert.ToInt32(r["edit_img_poster2_count"].ToString());
                        smrProgramInfo.edit_img_banner_count = Convert.ToInt32(r["edit_img_banner_count"].ToString());
                        smrProgramInfo.edit_img_thumb_count = Convert.ToInt32(r["edit_img_thumb_count"].ToString());

                        status = r["status"].ToString();
                        if (status.Equals("Pending"))
                        {
                            //log.logging("[ProgramService] imgsrcpath : " + imgsrcpath);
                            //log.logging("[ProgramService] orgimgname : " + orgimgname);

                            // 조건 다각도로 점검할것!!!!!
                            mapper.SetArchiveSmrProgramServiceRunning(smrProgramInfo.pid);

                            //frmMain.WriteLogThread(String.Format(@"[ProgramArchiveService] pid ({0}) is Archive", pid));
                            log.logging(String.Format("{0} is archiving",smrProgramInfo.pid) );

                            // 스포츠, 예능 구분해야함(프로그램 정보로부터 가져올 수 있음)
                            StringBuilder sb = new StringBuilder();
                            if (Singleton.getInstance().Test)
                            {
                                sb.Append(@"Z:\mbcplus\archive\test\smr_program");
                            } else
                            {
                                sb.Append(@"Z:\mbcplus\archive\smr_program");
                            }
                            sb.Append(Path.DirectorySeparatorChar);                            
                            sb.Append(smrProgramInfo.pid);

                            smrProgramInfo.targetpath = sb.ToString();
                            log.logging(String.Format("{0}", smrProgramInfo.ToString() ));
                                
                            try
                            {
                                if (!Directory.Exists(smrProgramInfo.targetpath))
                                {
                                    Directory.CreateDirectory(smrProgramInfo.targetpath);
                                }
                            }
                            catch (Exception e)
                            {
                                frmMain.WriteLogThread(e.ToString());
                            }
                            /*
                            connPool.ConnectionOpen();
                            m_sql = String.Format(@"INSERT INTO TB_ARCHIVE(insert_time, pid, srcpath, targetpath, type, status)
                                            VALUES (CURRENT_TIMESTAMP(), '{0}', '{1}', '{2}', 'IMG', 'Pending')", pid, Util.escapedPath(imgsrcpath), Util.escapedPath(targetPath + Path.DirectorySeparatorChar + pid + Path.GetExtension(orgimgname.ToLower())));

                            cmd = new MySqlCommand(m_sql, connPool.getConnection());
                            cmd.ExecuteNonQuery();
                            connPool.ConnectionClose();
                            */
                            mapper.ArchiveSmrProgram(smrProgramInfo);
                            
                            //이미지가 없을 땐 Status를 Completed로 변경
                            mapper.UpdateSmrProgramStatus ("Completed", smrProgramInfo.pid);
                            //frmMain.WriteLogThread("[ProgramSmrService] " + smrProgramInfo.pid + " img not found");
                            //log.logging(String.Format("{0} img is not found", smrProgramInfo.pid) );                            
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
