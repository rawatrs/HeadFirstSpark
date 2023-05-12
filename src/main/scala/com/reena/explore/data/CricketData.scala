package com.reena.explore.data
import java.time.Instant

object CricketData {
    case class Cricket(Batsman:         Int,
                       Batsman_Name:    String,
                       Bowler:          Int,
                       Bowler_Name:     String,
                       Commentary:      String,
                       Detail:          String,
                       Dismissed:       Int,
                       Id:              Int,
                       Isball:          Boolean,
                       Isboundary:      Int,
                       Iswicket:        Int,
                       Over:            Double,
                       Runs:            Int,
                       Timestamp:       String)
}
