package com.logicalgenetics.voting.model

case class Score(beerId : Int = 0, score : Int = 0, count : Int = 1){
  def +(that: Score): Score = Score(this.beerId, this.score + that.score, this.count + that.count)
  def -(that: Score): Score = Score(this.beerId, this.score - that.score, this.count - that.count)
}
