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
    class ArchiveProgramService
    {
        private Boolean _shouldStop = false;                     
        private String m_pk;        
        //private String m_sql = "";
        //private ConnectionPool connPool;
        private SqlMapper mapper;

        private Log log;

        public ArchiveProgramService()
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

            //connPool = new ConnectionPool();
            //connPool.SetConnection(new MySqlConnection(Singleton.getInstance().GetStrConn()));

            //String strBaseUri = "http://metaapi.mbcmedia.net:5000/SMRMetaCollect.svc/";
            
            //Waiting for make winform
            Thread.Sleep(5000);
            //frmMain.WriteLogThread("Programseq Archive Service Start...");
            log.logging("Service Start...");

            while (!_shouldStop)
            {
                try
                {
                    mapper.GetProgramService(ds);

                    foreach (DataRow r in ds.Tables[0].Rows)
                    {
                        vo.ProgramInfo programInfo = new vo.ProgramInfo();
                        
                        m_pk = r["pk"].ToString();
                        
                        programInfo.pid = r["pid"].ToString();
                        programInfo.img = r["src_img"].ToString();
                        programInfo.org_img = r["org_img"].ToString();                        
                        programInfo.posterimg = r["src_poster_img"].ToString();
                        programInfo.org_posterimg = r["org_poster_img"].ToString();
                        programInfo.thumbimg = r["src_thumb_img"].ToString();
                        programInfo.org_thumbimg = r["org_thumb_img"].ToString();
                        programInfo.circleimg = r["src_circle_img"].ToString();
                        programInfo.org_circleimg = r["org_circle_img"].ToString();
                        programInfo.highresimg = r["src_highres_img"].ToString();
                        programInfo.org_highresimg = r["org_highres_img"].ToString();
                        programInfo.logoimg = r["src_logo_img"].ToString();
                        programInfo.org_logoimg = r["org_logo_img"].ToString();
                        programInfo.edit_img_count = Convert.ToInt32(r["edit_img_count"].ToString());
                        programInfo.edit_img_poster_count = Convert.ToInt32(r["edit_img_poster_count"].ToString());
                        programInfo.edit_img_thumb_count = Convert.ToInt32(r["edit_img_thumb_count"].ToString());
                        programInfo.edit_img_circle_count = Convert.ToInt32(r["edit_img_circle_count"].ToString());
                        programInfo.edit_img_highres_count = Convert.ToInt32(r["edit_img_highres_count"].ToString());
                        programInfo.edit_img_logo_count = Convert.ToInt32(r["edit_img_logo_count"].ToString());

                        status = r["status"].ToString();
                        if (status.Equals("Pending"))
                        {
                            //log.logging("[ProgramService] imgsrcpath : " + imgsrcpath);
                            //log.logging("[ProgramService] orgimgname : " + orgimgname);
                            
                            mapper.UpdateArchiveProgramServiceRunning(programInfo.pid);

                            frmMain.WriteLogThread(String.Format(@"[ProgramArchiveService] pid ({0}) is Archive", programInfo.pid));                            

                            // 스포츠, 예능 구분해야함(프로그램 정보로부터 가져올 수 있음)
                            StringBuilder sb = new StringBuilder();
                            if ( Singleton.getInstance().Test )
                            {
                                sb.Append(@"Z:\mbcplus\archive\test\program");
                            }
                            else
                            {
                                sb.Append(@"Z:\mbcplus\archive\program");
                            }
                            sb.Append(Path.DirectorySeparatorChar);
                            sb.Append(programInfo.pid);

                            programInfo.targetpath = sb.ToString();

                            frmMain.WriteLogThread(programInfo.targetpath);
                            
                            try
                            {
                                if (!Directory.Exists(programInfo.targetpath))
                                {
                                    Directory.CreateDirectory(programInfo.targetpath);
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

                            mapper.ArchiveProgram(programInfo);
                            
                            //이미지가 없을 땐 Status를 Completed로 변경
                            mapper.UpdateProgramStatus("Completed", programInfo.pid);
                            //frmMain.WriteLogThread("[ProgramService] " + pid + " img not found");
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
            //connPool.ConnectionDisPose();
            log.logging("Thread Terminate");
        }
    }
}
