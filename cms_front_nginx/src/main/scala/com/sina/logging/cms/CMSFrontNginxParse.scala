package com.sina.logging.cms

/**
  * Created by jianping on 2017/6/7.
  */
object CMSFrontNginxParse {
  def main(args: Array[String]): Unit = {
    val s = "{| NGINX[5816]: [07/Jun/2017:11:20:18 +0800]`-`\"GET /newsapp/activities/msgbox.d.json?did=2cd0f01a09cc2f834a0cc121dc18191c&uid=&vn=621&act=get&from=6062193012 HTTP/1.1\"`\"-\"`200`[10.13.216.57]`-`\"-\"`0.006`85`-`10.13.216.57`i.interface.sina.cn`-|10.13.3.44|web003.nwapcms.msina.bx.sinanode.com|\"}"

    val tmp1 = s.split("`")
    val tmp2 = s.split("\\|")

    for (t <- tmp1) println(t)
    for (t <- tmp2) println(t)

    val apiName = tmp1(2).split(" ")(1).split("\\?")(0) // "GET /newsapp/activities/msgbox.d.json?did=2cd0f01a09cc2f834a0cc121dc18191c&uid=&vn=621&act=get&from=6062193012 HTTP/1.1"
    val status = tmp1(4)
    val requestTime = tmp1(8)
    val apiDomain = tmp1(12)
    val hostName = tmp2(tmp2.length-2)
    val idcNameTemp = hostName.split("\\.") //web003.nwapcms.msina.bx.sinanode.com
    val idcName = idcNameTemp(idcNameTemp.length-3)
    println("========================================")
    println(s"apiDomain : $apiDomain")
    println(s"apiName : $apiName")
    println(s"requestTime : $requestTime")
    println(s"status : $status")
    println(s"idcName : $idcName")
    println(s"hostName : $hostName")
  }


}
