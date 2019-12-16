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
    class ProgramseqService
    {
        private Boolean _shouldStop = false;                     
        private String m_pk;        
        private String m_sql = "";
        private ConnectionPool connPool;
        private SqlMapper mapper;

        private Log log;

        public ProgramseqService()
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
            MySqlCommand cmd;

            connPool = new ConnectionPool();
            connPool.SetConnection(new MySqlConnection(Singleton.getInstance().GetStrConn()));

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
                        m_pk = r["pk"].ToString();
                        String pid = r["pid"].ToString();
                        // ADD log
                        String imgsrcpath = r["imgsrcpath"].ToString();
                        String orgimgname = r["orgimgname"].ToString();

                        status = r["status"].ToString();
                        if (status.Equals("Pending"))
                        {
                            //log.logging("[ProgramService] imgsrcpath : " + imgsrcpath);
                            //log.logging("[ProgramService] orgimgname : " + orgimgname);

                            if (!String.IsNullOrEmpty(imgsrcpath))
                            {
                                mapper.UpdateArchiveProgramServiceRunning(pid);

                                frmMain.WriteLogThread(String.Format(@"[ProgramArchiveService] pid ({0}) is Archive", pid));
                                String targetPath = "";

                                // 스포츠, 예능 구분해야함(프로그램 정보로부터 가져올 수 있음)
                                StringBuilder sb = new StringBuilder();
                                sb.Append(@"Z:\mbcplus\archive\program");
                                sb.Append(Path.DirectorySeparatorChar);
                                sb.Append(pid);

                                targetPath = sb.ToString();

                                frmMain.WriteLogThread(targetPath);

                                if (!String.IsNullOrEmpty(imgsrcpath))
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
                                    m_sql = String.Format(@"INSERT INTO TB_ARCHIVE(insert_time, pid, srcpath, targetpath, type, status)
                                                  VALUES (CURRENT_TIMESTAMP(), '{0}', '{1}', '{2}', 'IMG', 'Pending')", pid, Util.escapedPath(imgsrcpath), Util.escapedPath(targetPath + Path.DirectorySeparatorChar + pid + Path.GetExtension(orgimgname.ToLower())));

                                    cmd = new MySqlCommand(m_sql, connPool.getConnection());
                                    cmd.ExecuteNonQuery();
                                    connPool.ConnectionClose();
                                }
                            }
                            else
                            {
                                //이미지가 없을 땐 Status를 Completed로 변경
                                mapper.UpdateProgramStatus("Completed", pid);
                                frmMain.WriteLogThread("[ProgramService] " + pid + " img not found");                                
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
            connPool.ConnectionDisPose();
            log.logging("Thread Terminate");
        }
    }
}
