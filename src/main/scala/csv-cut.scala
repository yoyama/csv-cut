import com.github.tototoshi.csv._
import java.io.File
import java.io.FileWriter
import sys.process._
import scala.util.control.Exception._

case class Opt(
  inputFile:File,
  line:Option[Int] = None,
  field:Option[List[Int]] = None,
  inNoHeader:Boolean = false,
  inEnc:Option[String] = None ,
  inDelimiter:Char = ',',
  outPrefix:String = "out",
  outSuffix:String = ".csv",
  outFields:Option[String] = None,
  outNoHeader:Boolean = false,
  outEnc:Option[String] = None,
  outCRLF:String = Opt.getCRLF(None),
  outDelimiter:Char = ',',
  debugEnable:Boolean = false
)
object Opt {
  val encMap = Map(
    'sjis -> "Windows-31J",
    'utf -> "UTF-8",
    'euc -> "EUC-JP"
  )
  val crlfMap = Map(
    'win -> "\r\n",
    'mac -> "\r",
    'unix -> "\n"
  )

  def getEnc(enc:Option[String]):String = {
    val v = for{ e <- enc; ee <- encMap.get(Symbol(e))} yield ee
    v.getOrElse(System.getProperty("file.encoding"))
  }

  def getCRLF(crlf:Option[String]):String = {
    val v = for{ e <- crlf; ee <- crlfMap.get(Symbol(e))} yield ee
    v.getOrElse(System.getProperty("line.separator"))
  }

  def getDelimiter(del:String):Char = {
    del match {
      case "TAB" | "\t" | "\\t"=> '\t'
      case "" => ','
      case x => x.toCharArray()(0)
    }
  }
}


case class OutFieldInfo(numColumn:Int, fields:List[Int])
object OutFieldInfo {
  def field(columns:List[String], outFields:OutFieldInfo):List[String] = {
    outFields.fields.flatMap{
      case f if(f <= columns.size && f >= 1) => List(columns(f-1))
      case _ => Nil
    }
  }
}

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

  def map2[X,Y,Z](ox:Option[X], oy:Option[Y])(f:(X,Y)=>Z):Option[Z] = {
    for{
      x <- ox
      y <- oy
    } yield f(x,y)
  }

  def run(opt:Opt):Int = {

    val outFieldInfo = opt.outFields.map(f => getFields(opt.inputFile, f))
    val outCsvFormat = new DefaultCSVFormat{
      override val lineTerminator = opt.outCRLF
      override val delimiter = opt.outDelimiter
    }
    val inCsvFormat = new DefaultCSVFormat{
      override val delimiter = opt.inDelimiter
    }

    val readerAll = CSVReader.open(opt.inputFile)(inCsvFormat).toStream
    val header_ = if(opt.inNoHeader) None else  Some(readerAll.head)
    val header = header_.map(h => outFieldInfo.map(of => OutFieldInfo.field(h,of)).getOrElse(h))
    val reader_ = if(opt.inNoHeader) readerAll else readerAll.tail
    val reader = outFieldInfo.map(of => reader_.map(c => OutFieldInfo.field(c, of))).getOrElse(reader_)

    val readers:List[Stream[List[String]]] = opt.line match {
      case Some(l) => reader.toStream.grouped(l).toList
      case None => List(reader.toStream)
    }

    val results = readers.zipWithIndex.map{
      case (r, idx) => catching(classOf[Throwable]) either {
        val outName = s"${opt.outPrefix}%03d${opt.outSuffix}".format(idx)
        Console.err.println(s"$outName")
        val writer = CSVWriter.open(new File(outName), Opt.getEnc(opt.outEnc))(outCsvFormat)
        if(!opt.outNoHeader) header.map(h => writer.writeRow(h))
        writer.writeAll(r)
        writer.close()
        (r, idx, outName)
      }
      case others => {
        Console.err.println(s"Unkown error:${others.toString}")
        Left(new Throwable(s"Unknown error: ${others.toString}"))
      }
    }
    val errResults = results.filter{
      case Left(e) => true
      case Right(v) => false
    }
    if(errResults.size > 0)
      1
    else
      0
  }

  def countColumn(inputFile:File):Int = {
    val readerAll = CSVReader.open(inputFile).toStream
    readerAll.head.size
  }

  def getFields(inputFile:File, fields:String):OutFieldInfo = {
    val c = countColumn(inputFile)
    val o = parseFieldOption(c, fields)
    OutFieldInfo(c, o)
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
    flist.sorted.distinct.filter(x => if(x >=1 && x <= numCols) true else false)
  }

  def parseArgs(args:Array[String]):Option[Opt] = {
    val parser = new scopt.OptionParser[Opt]("csv-cut") {
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
      } text("output fields. (ex1) -f 1,2,3 (ex2) -f 2-5 (ex3) -f 3-")

      opt[Unit]("no-header-in") action { (x, o) =>
        o.copy(inNoHeader = true) 
      } text("no header for input")

      opt[String]("delim-in") action { (x, o) =>
        o.copy(inDelimiter = Opt.getDelimiter(x))
      } text("input delimiter char. default ','. TAB,\\t,<tab> are recorgnized tab.")

      opt[Unit]("no-header-out") action { (x, o) =>
        o.copy(outNoHeader = true) 
      } text("no header for output")

      opt[String]("prefix-out") action { (x, o) =>
        o.copy(outPrefix = x)
      } text("prefix for output file name. default 'out'")

      opt[String]("suffix-out") action { (x, o) =>
        o.copy(outSuffix = x)
      } text("suffix for output file name. default '.csv'")

      opt[String]("enc-out") action { (x, o) =>
        o.copy(outEnc = Some(x))
      } validate { v =>
        Opt.encMap.get(Symbol(v)) match {
          case Some(enc)  => success
          case _ => failure(s"Invalid param enc-out $v")
        }
      } text("output encode (utf | sjis | euc)")

      opt[String]("crlf-out") action { (x, o) =>
        o.copy(outCRLF = Opt.getCRLF(Some(x)))
      } validate { v =>
        Opt.crlfMap.get(Symbol(v)) match {
          case Some(crlf)  => success
          case _ => failure(s"Invalid param crlf-out $v")
        }
      } text("output crlf  (unix | win | mac)")

      opt[String]("delim-out") action { (x, o) =>
        o.copy(outDelimiter = Opt.getDelimiter(x))
      } text("output delimiter char. default ','. TAB,\\t,<tab> are recorgnized tab.")

      opt[Unit]("debug") action { (x, o) =>
        o.copy(debugEnable = true)
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
