package com.github.dmarcous.ddbgscan.api

import com.github.dmarcous.ddbgscan.core.AlgorithmParameters

case class AlgorithmConfig(
    inputPath : String,
    outputFolderPath : String,
    parameters : AlgorithmParameters
)
{
}