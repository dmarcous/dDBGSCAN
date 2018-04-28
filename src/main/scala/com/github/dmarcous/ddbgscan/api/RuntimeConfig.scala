package com.github.dmarcous.ddbgscan.api

import com.github.dmarcous.ddbgscan.core.{AlgorithmParameters, IOConfig}

case class RuntimeConfig(
    ioConfig: IOConfig,
    parameters : AlgorithmParameters
)
{
}