package com.util

import java.io.File

import com.typesafe.config.ConfigFactory

/**
 * Created by seawalker on 2015/8/13.
 */
object LocalConfHelper {
  def getConfig(confPath:String): com.typesafe.config.Config ={
    val confFile = ConfigFactory.parseFile(new File(confPath))
    ConfigFactory.load(confFile)
  }
}
