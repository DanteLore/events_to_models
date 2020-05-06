package com.logicalgenetics.voting

import com.logicalgenetics.model.Score
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.matchers.should.Matchers


trait TestFixture extends AnyFlatSpec with Matchers with BeforeAndAfterEach with BeforeAndAfterAll {

}

class VotingTests extends TestFixture {
  "Scores" should "add up" in {
    val sum = Score(beerId = 1, score = 1, count = 1) + Score(beerId = 2, score = 2, count = 2)

    sum shouldBe Score(beerId = 1, score = 3, count = 3)
  }

  "Scores" should "subtract" in {
    val sum = Score(beerId = 4, score = 4, count = 4) + Score(beerId = 1, score = 1, count = 1)

    sum shouldBe Score(beerId = 4, score = 3, count = 3)
  }


}
