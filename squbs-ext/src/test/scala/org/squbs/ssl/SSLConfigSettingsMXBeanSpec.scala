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

import java.net.URL

import akka.actor.ActorSystem
import akka.testkit.TestKit
import com.typesafe.sslconfig.akka.AkkaSSLConfig
import com.typesafe.sslconfig.ssl.ClientAuth.Need
import com.typesafe.sslconfig.ssl._
import org.scalatest.{FlatSpecLike, Matchers}

class SSLConfigSettingsMXBeanSpec extends TestKit(ActorSystem("SSLConfigSettingsMXBeanSpec"))
with FlatSpecLike with Matchers {

  it should "show the SSLConfigSettings default values correctly" in {
    val sslConfigSettingsMXBean = SSLConfigSettingsMXBeanImpl("sample", AkkaSSLConfig().config)

    sslConfigSettingsMXBean.name shouldBe "sample"
    sslConfigSettingsMXBean.getDefault shouldBe false
    sslConfigSettingsMXBean.getProtocol shouldBe "TLSv1.2"
    sslConfigSettingsMXBean.getCheckRevocation shouldBe None.toString
    sslConfigSettingsMXBean.getRevocationLists shouldBe None.toString
    sslConfigSettingsMXBean.getEnabledCipherSuites shouldBe None.toString
    sslConfigSettingsMXBean.getEnabledProtocols shouldBe "TLSv1.2,TLSv1.1,TLSv1"
    sslConfigSettingsMXBean.getDisabledKeyAlgorithms shouldBe "RSA keySize < 2048,DSA keySize < 2048,EC keySize < 224"
    sslConfigSettingsMXBean.getHostnameVerifierClass shouldBe "com.typesafe.sslconfig.ssl.DefaultHostnameVerifier"
    sslConfigSettingsMXBean.getClientAuth shouldBe "default"
    sslConfigSettingsMXBean.getProtocols shouldBe 'empty
    sslConfigSettingsMXBean.getKeyManagerAlgorithm shouldBe "SunX509"
    sslConfigSettingsMXBean.getKeyStoreConfigs shouldBe 'empty
    sslConfigSettingsMXBean.getTrustManagerAlgorithm shouldBe "PKIX"
    sslConfigSettingsMXBean.getTrustStoreConfigs shouldBe 'empty
    sslConfigSettingsMXBean.getSSLDebugAll shouldBe false
    sslConfigSettingsMXBean.getSSLDebugSsl shouldBe false
    sslConfigSettingsMXBean.getSSLDebugCertpath shouldBe false
    sslConfigSettingsMXBean.getSSLDebugOcsp shouldBe false
    sslConfigSettingsMXBean.getSSLDebugRecordOptions shouldBe None.toString
    sslConfigSettingsMXBean.getSSLDebugHandshakeOptions shouldBe None.toString
    sslConfigSettingsMXBean.getSSLDebugKeygen shouldBe false
    sslConfigSettingsMXBean.getSSLDebugSession shouldBe false
    sslConfigSettingsMXBean.getSSLDebugDefaultctx shouldBe false
    sslConfigSettingsMXBean.getSSLDebugSslctx shouldBe false
    sslConfigSettingsMXBean.getSSLDebugSessioncache shouldBe false
    sslConfigSettingsMXBean.getSSLDebugKeymanager shouldBe false
    sslConfigSettingsMXBean.getSSLDebugTrustmanager shouldBe false
    sslConfigSettingsMXBean.getSSLDebugPluggability shouldBe false
    sslConfigSettingsMXBean.getAllowWeakProtocols shouldBe false
    sslConfigSettingsMXBean.getAllowWeakCiphers shouldBe false
    sslConfigSettingsMXBean.getAllowLegacyHelloMessages shouldBe None.toString
    sslConfigSettingsMXBean.getAllowUnsafeRenegotiation shouldBe None.toString
    sslConfigSettingsMXBean.getDisableHostnameVerification shouldBe false
    sslConfigSettingsMXBean.getDisableSNI shouldBe false
    sslConfigSettingsMXBean.getAcceptAnyCertificate shouldBe false
  }

  it should "show the SSLConfigSettings with non-default values correctly" in {

    val sslConfigSettings = AkkaSSLConfig().config.
      withDefault(true).
      withProtocol("TLSv1.1").
      withCheckRevocation(Some(false)).
      withRevocationLists(Some(new URL("http://www.abc.com") :: new URL("http://www.def.com") :: Nil)).
      withEnabledCipherSuites(Some("TLS_AES_128_GCM_SHA256" :: Nil)).
      withEnabledProtocols(Some("TLSv1.2" :: "TLSv1.1" :: Nil)).
      withDisabledKeyAlgorithms("RSA keySize < 2048" :: Nil).
      withSslParametersConfig(SSLParametersConfig().withClientAuth(Need).withProtocols("TLSv1.2" :: Nil)).
      withKeyManagerConfig(KeyManagerConfig().
        withKeyStoreConfigs(KeyStoreConfig(Some("abc"), None).withStoreType("JKS") ::
                            KeyStoreConfig(None, Some("abc")).withStoreType("P12"):: Nil)).
      withTrustManagerConfig(TrustManagerConfig().
        withTrustStoreConfigs(TrustStoreConfig(Some("abc"), None).withStoreType("JKS") ::
                              TrustStoreConfig(None, Some("abc")).withStoreType("P12"):: Nil)).
      withDebug(SSLDebugConfig().
        withHandshake(Some(SSLDebugHandshakeOptions().withData(true))).
        withRecord(Some(SSLDebugRecordOptions().withPlaintext(true))).
        withAll(true).
        withCertPath(true).
        withDefaultContext(true).
        withKeygen(true).
        withKeymanager(true).
        withOcsp(true).
        withPluggability(true).
        withSession(true).
        withSessioncache(true).
        withSsl(true).
        withSslctx(true).
        withTrustmanager(true)).
      withLoose(SSLLooseConfig().
        withAllowLegacyHelloMessages(Some(true)).
        withAllowUnsafeRenegotiation(Some(true)).
        withAcceptAnyCertificate(true).
        withAllowWeakCiphers(true).
        withAllowWeakProtocols(true).
        withDisableHostnameVerification(true).
        withDisableSNI(true))

    val sslConfigSettingsMXBean = SSLConfigSettingsMXBeanImpl("sample", sslConfigSettings)

    sslConfigSettingsMXBean.name shouldBe "sample"
    sslConfigSettingsMXBean.getDefault shouldBe true
    sslConfigSettingsMXBean.getProtocol shouldBe "TLSv1.1"
    sslConfigSettingsMXBean.getCheckRevocation shouldBe false.toString
    sslConfigSettingsMXBean.getRevocationLists shouldBe "http://www.abc.com,http://www.def.com"
    sslConfigSettingsMXBean.getEnabledCipherSuites shouldBe "TLS_AES_128_GCM_SHA256"
    sslConfigSettingsMXBean.getEnabledProtocols shouldBe "TLSv1.2,TLSv1.1"
    sslConfigSettingsMXBean.getDisabledKeyAlgorithms shouldBe "RSA keySize < 2048"
    sslConfigSettingsMXBean.getHostnameVerifierClass shouldBe "com.typesafe.sslconfig.ssl.DefaultHostnameVerifier"
    sslConfigSettingsMXBean.getClientAuth shouldBe "need"
    sslConfigSettingsMXBean.getProtocols shouldBe "TLSv1.2"
    sslConfigSettingsMXBean.getKeyManagerAlgorithm shouldBe "SunX509"
    sslConfigSettingsMXBean.getKeyStoreConfigs shouldBe "data=abc storeType=JKS,filePath=abc storeType=P12"
    sslConfigSettingsMXBean.getTrustManagerAlgorithm shouldBe "PKIX"
    sslConfigSettingsMXBean.getTrustStoreConfigs shouldBe "data=abc storeType=JKS,filePath=abc storeType=P12"
    sslConfigSettingsMXBean.getSSLDebugAll shouldBe true
    sslConfigSettingsMXBean.getSSLDebugSsl shouldBe true
    sslConfigSettingsMXBean.getSSLDebugCertpath shouldBe true
    sslConfigSettingsMXBean.getSSLDebugOcsp shouldBe true
    sslConfigSettingsMXBean.getSSLDebugRecordOptions shouldBe "packet=false plaintext=true"
    sslConfigSettingsMXBean.getSSLDebugHandshakeOptions shouldBe "data=true verbose=false"
    sslConfigSettingsMXBean.getAllowWeakProtocols shouldBe true
    sslConfigSettingsMXBean.getAllowWeakCiphers shouldBe true
    sslConfigSettingsMXBean.getAllowLegacyHelloMessages shouldBe true.toString
    sslConfigSettingsMXBean.getAllowUnsafeRenegotiation shouldBe true.toString
    sslConfigSettingsMXBean.getDisableHostnameVerification shouldBe true
    sslConfigSettingsMXBean.getDisableSNI shouldBe true
    sslConfigSettingsMXBean.getAcceptAnyCertificate shouldBe true
  }
}
