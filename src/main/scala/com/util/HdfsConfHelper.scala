package com.util

import com.typesafe.config.ConfigFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

/**
 * Created by seawalker on 2015/8/13.
 */
object HdfsConfHelper {
  def getConfig(confPath:String): com.typesafe.config.Config ={
    val confFile = ConfigFactory.parseString(read(confPath))
    ConfigFactory.load(confFile)
  }

  def read(path:String):String = {
    val conf = new Configuration()
    val filePath = new Path(path)
    val fileSystem = FileSystem.get(conf)
    val fsdin = fileSystem.open(filePath)
    val status = fileSystem.getFileStatus(filePath)
    try{
      val len = status.getLen.toInt
      val buffer = new Array[Byte](len)
      fsdin.readFully(0.toLong,buffer,0,len)
      new String(buffer)
    }finally {
      fsdin.close()
      fileSystem.close()
    }
  }
}
