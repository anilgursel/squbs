/*
 * Copyright 2015 PayPal
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.squbs.ssl

import javax.management.MXBean

import com.typesafe.sslconfig.ssl.SSLConfigSettings

@MXBean
trait SSLConfigSettingsMXBean {
  def getName: String
  def getDefault: Boolean
  def getProtocol: String
  def getCheckRevocation: String
  def getRevocationLists: String
  def getEnabledCipherSuites: String
  def getEnabledProtocols: String
  def getDisabledSignatureAlgorithms: String
  def getDisabledKeyAlgorithms: String
  def getClientAuth: String
  def getProtocols: String
  def getKeyManagerAlgorithm: String
  def getKeyStoreConfigs: String
  def getTrustManagerAlgorithm: String
  def getTrustStoreConfigs: String
  def getHostnameVerifierClass: String
  def getSecureRandom: String
  def getSSLDebugAll: Boolean
  def getSSLDebugCertpath: Boolean
  def getSSLDebugDefaultctx: Boolean
  def getSSLDebugHandshakeOptions: String
  def getSSLDebugKeygen: Boolean
  def getSSLDebugKeymanager: Boolean
  def getSSLDebugOcsp: Boolean
  def getSSLDebugPluggability: Boolean
  def getSSLDebugRecordOptions: String
  def getSSLDebugSession: Boolean
  def getSSLDebugSessioncache: Boolean
  def getSSLDebugSsl: Boolean
  def getSSLDebugSslctx: Boolean
  def getSSLDebugTrustmanager: Boolean
  def getAcceptAnyCertificate: Boolean
  def getAllowLegacyHelloMessages: String
  def getAllowUnsafeRenegotiation: String
  def getAllowWeakCiphers: Boolean
  def getAllowWeakProtocols: Boolean
  def getDisableHostnameVerification: Boolean
  def getDisableSNI: Boolean
}

case class SSLConfigSettingsMXBeanImpl(name: String, sslConfigSettings: SSLConfigSettings)
  extends SSLConfigSettingsMXBean {

  val `None` = "None"
  val Seperator = ","

  override def getName: String = name

  override def getDefault: Boolean = sslConfigSettings.default

  override def getProtocol: String = sslConfigSettings.protocol

  override def getCheckRevocation: String = sslConfigSettings.checkRevocation.map(_.toString).getOrElse(`None`)

  override def getRevocationLists: String =
    sslConfigSettings.revocationLists.map(_.map(_.toString).mkString(Seperator)).getOrElse(`None`)

  override def getEnabledCipherSuites: String =
    sslConfigSettings.enabledCipherSuites.map(_.mkString(Seperator)).getOrElse(`None`)

  override def getEnabledProtocols: String =
    sslConfigSettings.enabledProtocols.map(_.mkString(Seperator)).getOrElse(`None`)

  override def getDisabledSignatureAlgorithms: String =
    sslConfigSettings.disabledSignatureAlgorithms.mkString(Seperator)

  override def getDisabledKeyAlgorithms: String = sslConfigSettings.disabledKeyAlgorithms.mkString(Seperator)

  override def getClientAuth: String = sslConfigSettings.sslParametersConfig.clientAuth.toString.toLowerCase

  override def getProtocols: String = sslConfigSettings.sslParametersConfig.protocols.mkString(Seperator)

  override def getKeyManagerAlgorithm: String = sslConfigSettings.keyManagerConfig.algorithm

  override def getKeyStoreConfigs: String = sslConfigSettings.keyManagerConfig.keyStoreConfigs map { config =>
    val data = config.data.map(d => s"data=$d").getOrElse("")
    val filePath = config.filePath.map(fp => s"filePath=$fp").getOrElse("")
    data + filePath + s" storeType=${config.storeType}" // Do not include password
  } mkString(Seperator)

  override def getTrustManagerAlgorithm: String = sslConfigSettings.trustManagerConfig.algorithm

  override def getTrustStoreConfigs: String = sslConfigSettings.trustManagerConfig.trustStoreConfigs map { config =>
    val data = config.data.map(d => s"data=$d").getOrElse("")
    val filePath = config.filePath.map(fp => s"filePath=$fp").getOrElse("")
    data + filePath + s" storeType=${config.storeType}" // Do not include password
  } mkString(Seperator)

  override def getHostnameVerifierClass: String = sslConfigSettings.hostnameVerifierClass.getName

  override def getSecureRandom: String = sslConfigSettings.secureRandom.map { secureRandom =>
    "class=" + secureRandom.getClass.getName +
      " algorithm=" + secureRandom.getAlgorithm +
      " provider=" + secureRandom.getProvider.getName
  } getOrElse `None`

  override def getSSLDebugAll: Boolean = sslConfigSettings.debug.all

  override def getSSLDebugCertpath: Boolean = sslConfigSettings.debug.certpath

  override def getSSLDebugDefaultctx: Boolean = sslConfigSettings.debug.defaultctx

  override def getSSLDebugHandshakeOptions: String = sslConfigSettings.debug.handshake.map { options =>
    "data=" + options.data + " verbose=" + options.verbose
  } getOrElse `None`

  override def getSSLDebugKeygen: Boolean = sslConfigSettings.debug.keygen

  override def getSSLDebugKeymanager: Boolean = sslConfigSettings.debug.keymanager

  override def getSSLDebugOcsp: Boolean = sslConfigSettings.debug.ocsp

  override def getSSLDebugPluggability: Boolean = sslConfigSettings.debug.pluggability

  override def getSSLDebugRecordOptions: String = sslConfigSettings.debug.record.map { options =>
    "packet=" + options.packet + " plaintext=" + options.plaintext
  } getOrElse `None`

  override def getSSLDebugSession: Boolean = sslConfigSettings.debug.session

  override def getSSLDebugSessioncache: Boolean = sslConfigSettings.debug.sessioncache

  override def getSSLDebugSsl: Boolean = sslConfigSettings.debug.ssl

  override def getSSLDebugSslctx: Boolean = sslConfigSettings.debug.sslctx

  override def getSSLDebugTrustmanager: Boolean = sslConfigSettings.debug.trustmanager

  override def getAcceptAnyCertificate: Boolean = sslConfigSettings.loose.acceptAnyCertificate

  override def getAllowLegacyHelloMessages: String =
    sslConfigSettings.loose.allowLegacyHelloMessages.map(_.toString).getOrElse(`None`)

  override def getAllowUnsafeRenegotiation: String =
    sslConfigSettings.loose.allowUnsafeRenegotiation.map(_.toString).getOrElse(`None`)

  override def getAllowWeakCiphers: Boolean = sslConfigSettings.loose.allowWeakCiphers

  override def getAllowWeakProtocols: Boolean = sslConfigSettings.loose.allowWeakProtocols

  override def getDisableHostnameVerification: Boolean = sslConfigSettings.loose.disableHostnameVerification

  override def getDisableSNI: Boolean = sslConfigSettings.loose.disableSNI

}
