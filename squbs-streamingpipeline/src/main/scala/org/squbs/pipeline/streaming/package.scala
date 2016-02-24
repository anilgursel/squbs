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

package org.squbs.pipeline

import akka.stream.scaladsl.Flow

package object streaming {

  // (inbound flow, outbound flow, defaults on/off )
  type PipelineSetting = (Option[Seq[String]], Option[Seq[String]], Option[Boolean])

  type PipelineFlow = Flow[RequestContext, RequestContext, Unit]
}
