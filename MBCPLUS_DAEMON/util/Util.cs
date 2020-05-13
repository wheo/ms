using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using System.Runtime.Serialization.Formatters.Binary;

namespace MBCPLUS_DAEMON
{
    public static class Util
    {
        public static List<T> CloneList<T>(List<T> oldList)
        {
            BinaryFormatter formatter = new BinaryFormatter();
            MemoryStream stream = new MemoryStream();
            formatter.Serialize(stream, oldList);
            stream.Position = 0;
            return (List<T>)formatter.Deserialize(stream);
        }

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

        public static String getTestPath() {
            StringBuilder sb = new StringBuilder();

            sb.Append(@"Z:\mbcplus\archive");
            sb.Append(Path.DirectorySeparatorChar);
            sb.Append("test");
            sb.Append(Path.DirectorySeparatorChar);
            return sb.ToString();
        }

        public static String getSectionPath(String section)
        {
            StringBuilder sb = new StringBuilder();

            sb.Append(@"Z:\mbcplus\archive");
            sb.Append(Path.DirectorySeparatorChar);
            if (section == "S100")
            {
                sb.Append("drama");
            }
            else if (section == "S200")
            {
                // 예능
                sb.Append("ent");
            }
            else if (section == "S300")
            {
                // 교양
                sb.Append("refinement");
            }
            else if (section == "S400")
            {
                // 시사
                sb.Append("sisa");
            }
            else if (section == "S500")
            {
                // 스포츠
                sb.Append("sports");
            }
            else if (section == "S600")
            {
                // 어린이
                sb.Append("kid");
            }
            else if (section == "S700")
            {
                // 뮤직
                sb.Append("music");
            }
            else if (section == "S800")
            {
                // 음악
                sb.Append("game");
            }
            sb.Append(Path.DirectorySeparatorChar);                        
            return sb.ToString();
        }
    }
}