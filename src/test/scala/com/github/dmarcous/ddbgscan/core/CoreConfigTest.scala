package com.github.dmarcous.ddbgscan.core

import com.github.dmarcous.ddbgscan.core.CoreConfig._
import org.scalatest.FlatSpec
import org.scalatest.Matchers._

class CoreConfigTest extends FlatSpec{

  val NON_EXISTING_FUNCTION_CODE = -1

  "NEIGHBOUR_SIMILARITY_EXTENSION_FUNCTION_TRANSLATOR" should "translate function codes" in
  {

    CoreConfig.NEIGHBOUR_SIMILARITY_EXTENSION_FUNCTION_TRANSLATOR
      .get(DEFAULT_NEIGHBOUR_SIMILARITY_EXTENSION_FUNCTION_CODE).get should equal(
      DEFAULT_NEIGHBOUR_SIMILARITY_EXTENSION_FUNCTION)

    CoreConfig.NEIGHBOUR_SIMILARITY_EXTENSION_FUNCTION_TRANSLATOR
      .get(GEO_DISTANCE_UNDER_500m_SIMILARITY_EXTENSION_FUNCTION_CODE).get should equal(
      GEO_DISTANCE_UNDER_500m)

    CoreConfig.NEIGHBOUR_SIMILARITY_EXTENSION_FUNCTION_TRANSLATOR(NON_EXISTING_FUNCTION_CODE) should equal(
      DEFAULT_NEIGHBOUR_SIMILARITY_EXTENSION_FUNCTION)

  }
}
