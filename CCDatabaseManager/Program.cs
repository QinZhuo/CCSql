using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Xml;


namespace CCSql
{
    class Program
    {
      
        static void Main(string[] args)
        {
            Manager manager = new Manager();
            if (args.Length > 0)
            {
                manager.Start(args[0]);
            }
            else
            {
                manager.Start("root");
            }
            Console.ReadLine();
            
          
        }
    }
}
