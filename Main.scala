import java.util.{Properties, UUID}



object Main {
  def main(args: Array[String]): Unit = {
    val tran = new Transfer()
    val s3Content = tran.readFile()
    tran.produceToKafka(s3Content)
    val handle = new Handle()
    handle.take()
  }
}