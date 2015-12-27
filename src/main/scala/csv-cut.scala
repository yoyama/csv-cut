import com.github.tototoshi.csv._
import java.io.File
import java.io.FileWriter
import sys.process._

case class Opt(
  inputFile:File,
  outputPrefix:String = "out", outputSuffix:String = ".csv",
  line:Option[Int] = None, field:Option[List[Int]] = None,
  outFields:Option[String] = None,
  inNoHeader:Boolean = false, outNoHeader:Boolean = false,
  inEnc:Option[String] = None ,outEnc:Option[String] = None,
  debugEnable:Boolean = false
)


object CsvCutMain {
  def main(args:Array[String]):Unit = {
    val opt = parseArgs(args)
    val ret = opt.map(run(_)).getOrElse(1)

    ret match {
      case 0 => println("Ok.")
      case _ => println("Error.")
    }
    sys.exit(ret)
  }

  def run(opt:Opt):Int = {
    println(s"run run run ${opt.inputFile.toString}")
    val readerAll = CSVReader.open(opt.inputFile).toStream
    val header = if(opt.inNoHeader) None else  Some(readerAll.head)
    val reader = if(opt.inNoHeader) readerAll else readerAll.tail 
    val readers:List[Stream[List[String]]] = opt.line match {
      case Some(l) => reader.toStream.grouped(l).toList
      case None => List(reader.toStream)
    }

    readers.zipWithIndex.map{
      case (r, idx) =>
        val outName = s"${opt.outputPrefix}%03d${opt.outputSuffix}".format(idx)
        println(outName)
        val writer = CSVWriter.open(new FileWriter(new File(outName)))
        if(!opt.outNoHeader) header.map(h => writer.writeRow(h))
        writer.writeAll(r)
      case others => println(s"ZZ $others.toString")
    }
    0
  }

  def parseFieldOption(numCols:Int, fstr:Option[String]):List[Int] = {
    fstr.map(parseFieldOption(numCols, _)).getOrElse( (1 to numCols).toList)
  }
  
  def parseFieldOption(numCols:Int, fstr:String):List[Int] = {
    val list = (1 to numCols).toList
    val flist = fstr.split(",").foldLeft(List[Int]()){(acc,v) => {
      val reg1 = """(\d+)""".r
      val reg2 = """(\d+)-(\d+)""".r
      val reg3 = """-(\d+)""".r
      val reg4 = """(\d+)-""".r
      v match {
        case reg1(n) => n.toInt::acc
        case reg2(n1,n2) => (n1.toInt to n2.toInt).toList ::: acc
        case reg3(n) => (1 to n.toInt).toList ::: acc
        case reg4(n) => (n.toInt to numCols).toList ::: acc
        case x => Console.err.println(s"Invalid field: $x");  acc
      }
    }}
    println(flist)
    flist.sorted.distinct.filter(x => if(x >=1 && x <= numCols) true else false)
  }

  def parseArgs(args:Array[String]):Option[Opt] = {
    val parser = new scopt.OptionParser[Opt]("scopt") {
      head("csv-cut", "0.0.1")
      opt[String]('i', "input") action { (x, o) =>
        println(x)
        o.copy(inputFile = new File(x))
      } validate { v =>
        if(new File(v).canRead ) success
        else failure(s"Invalid param $v")
      } text("input file name")

      opt[String]('l', "line") action { (x, o) =>
        o.copy(line = Some(x.toInt))
      } validate { v =>
        val regex = """(\d+)""".r
        v match {
          case regex(l) if(l.toInt > 0) => success
          case _ => failure(s"Invalid param $v")
        }
      } text("line for horizontal split")

      opt[String]('f', "field") action { (x, o) =>
        o.copy(outFields = Some(x))
      } validate { v =>
        val regex = """(\d|,|\-)+""".r
        v match {
          case regex(x)  => success
          case _ => failure(s"Invalid param f $v")
        }
      } text("output fields")
      
      opt[Unit]("no-header-out") action { (x, o) =>
        o.copy(outNoHeader = true) 
      } text("no header for output")

      opt[Unit]("no-header-in") action { (x, o) =>
        o.copy(inNoHeader = true) 
      } text("no header for input")

      opt[Unit]("debug") action { (x, c) =>
        c.copy(debugEnable = true)
      } text("enable debug")
      help("help") text("prints this usage text")
    }
    parser.parse(args, Opt(inputFile = null)) map { config =>
      Some(config)
    } getOrElse {
      None
    }
  }
}
