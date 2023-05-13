package com.reena.explore.data
import java.time.Instant

object CricketData {
    case class Cricket(Batsman:         Option[Int],
                       Batsman_Name:    Option[String],
                       Bowler:          Option[Int],
                       Bowler_Name:     Option[String],
                       Commentary:      Option[String],
                       Detail:          Option[String],
                       Dismissed:       Option[Int],
                       Id:              Option[Int],
                       Isball:          Option[Boolean],
                       Isboundary:      Option[Int],
                       Iswicket:        Option[Int],
                       Over:            Option[Double],
                       Runs:            Option[Int],
                       Timestamp:       Option[String])
}
