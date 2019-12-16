using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;

namespace MBCPLUS_DAEMON
{
    public static class Util
    {
        public static String escapedPath(String path)
        {
            return path.Replace(@"\", @"\\").Replace("'", @"''");
        }

        public static String replaceSpaceChar(String path)
        {
            return path.Replace(" ", "_");
        }

        public static String repaceInvalidFilename(String path)
        {
            String invalid = new String(Path.GetInvalidFileNameChars());
            foreach(char c in invalid)
            {
                path = path.Replace(c.ToString(), "");
            }
            return path;
        }

        public static String repaceInvalidPath(String path)
        {
            String invalid = new String(Path.GetInvalidPathChars());
            foreach(char c in invalid)
            {
                path = path.Replace(c.ToString(), "");
            }
            return path;
        }

        public static String GetCurrentDate()
        {
            DateTime dateTime = DateTime.UtcNow.Date;
            return dateTime.ToString("yyyyMMdd");
        }

        public static String GetCurrentDate(int addDay)
        {
            DateTime dateTime = DateTime.UtcNow.Date;
            return dateTime.AddDays(addDay).ToString("yyyyMMdd");
        }

        public static String getSectionPath(String section)
        {
            StringBuilder sb = new StringBuilder();

            sb.Append(@"Z:\mbcplus\archive");
            sb.Append(Path.DirectorySeparatorChar);
            if (section == "01")
            {
                sb.Append("drama");
            }
            else if (section == "02")
            {
                // 02 예능
                sb.Append("ent");
            }
            else if (section == "03")
            {
                // 03 교양
                sb.Append("refinement");
            }
            else if (section == "04")
            {
                // 04 시사
                sb.Append("sisa");
            }
            else if (section == "05")
            {
                // 05 스포츠
                sb.Append("sports");
            }
            else if (section == "06")
            {
                // 06 어린이
                sb.Append("kid");
            }
            else if (section == "07")
            {
                // 07 라디오
                sb.Append("radio");
            }
            else if (section == "08")
            {
                // 08 음악
                sb.Append("music");
            }
            sb.Append(Path.DirectorySeparatorChar);                        
            return sb.ToString();
        }
    }
}