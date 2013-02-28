import scala.io.Source
import scala.util.Random
import scala.collection.mutable.ListBuffer

import java.io._
import collection.SortedSet


package edu.ohsu.sonmezsysbio.cloudbreak.scripts {


object ApplyVariantsToFasta {
  def printToFile(f: java.io.File)(op: java.io.PrintWriter => Unit) {
    val p = new java.io.PrintWriter(f)
    try { op(p) } finally { p.close() }
  }

  abstract class Variation(val begin : Int, val end : Int) extends Ordering[Variation]  {

    def covers(o : Int) : Boolean =
      o >= begin && o <= end
    def endsAt(o : Int) : Boolean =
      o == end

    def processBaseInside(c : Char) : Iterator[Char]
    def processEnd() : Iterator[Char]

    def compare(x: Variation, y: Variation): Int = x.begin - y.begin
  }

  class Insertion(begin : Int, insertedString : String) extends Variation(begin, begin) {
    def processBaseInside(c: Char): Iterator[Char] = (insertedString + c).iterator

    def processEnd(): Iterator[Char] = Iterator()
  }

  class Deletion(begin : Int, end : Int) extends Variation(begin, end) {
    def processBaseInside(c : Char) : Iterator[Char] = Iterator()
    def processEnd() : Iterator[Char] = Iterator()

    override def toString = "2\t" + begin + "\t" + end + "\t" + "Deletion:" + begin + "-" + end + "\t" + (end - begin)
  }

  class Inversion(begin : Int, end : Int) extends Variation(begin, end) {
    var basesSpanned : Array[Char] = null
    var offsetInInversion = 0
    def processBaseInside(c : Char) : Iterator[Char] = {
      if (basesSpanned == null) {
        basesSpanned = new Array[Char](end - begin)
      }
      basesSpanned(end - begin - offsetInInversion - 1) = c
      offsetInInversion = offsetInInversion + 1
      Iterator()
    }

    def processEnd() : Iterator[Char] = {
      basesSpanned.iterator
    }
    override def toString = "2\t" + begin + "\t" + end + "\t" + "Inversion:" + begin + "-" + end + "\t" + (end - begin)
  }

  class Duplication(begin : Int, end : Int) extends Variation(begin, end) {
    var basesSpanned : Array[Char] = null
    var offsetInDuplication = 0
    def processBaseInside(c : Char) : Iterator[Char] = {
      if (basesSpanned == null) {
        basesSpanned = new Array[Char](end - begin)
        for (i <- 0 until (basesSpanned.length)) basesSpanned(i) = 'X'
      }
      basesSpanned(offsetInDuplication) = c
      offsetInDuplication = offsetInDuplication + 1
      Iterator(c)
    }

    def processEnd() : Iterator[Char] = {
      basesSpanned.iterator
    }

    override def toString = "2\t" + begin + "\t" + end + "\t" + "Duplication:" + begin + "-" + end + "\t" + (end - begin)
  }

  class Alu(val begin : Int, val end : Int, family : String) {
    def formDeletionWith(right : Alu) : Deletion = {
      new Deletion(begin, right.begin)
    }
    def formInversionWith(right : Alu) : Inversion = {
      new Inversion(begin, right.begin)
    }
    def formDuplicationWith(right : Alu) : Duplication = {
      new Duplication(begin, right.begin)
    }
  }

  def readAlus(file : String) : List[Alu] = {
    val alus = new ListBuffer[Alu]()
    for (line <- Source.fromFile(file).getLines()) {
      val fields = line.split("\t")
      val alu = new Alu(fields(1).toInt, fields(2).toInt, fields(3))
      alus += alu
    }
    alus.toList
  }

  def genSVs(num : Int, alus : List[Alu]) : List[Variation] = {
    if (num == 0)
      List[Variation]()
    else {
      val leftIdx = Random.nextInt(alus.length - 1)
      val left = alus(leftIdx)
      for (right <- alus.slice(leftIdx + 1, alus.length)) {
        if (Random.nextInt(5) == 1) {
          val svType = Random.nextInt(3)
          svType match {
            case 0 => return left.formDeletionWith(right) :: genSVs(num - 1, alus)
            case 1 => return left.formInversionWith(right) :: genSVs(num - 1, alus)
            case 2 => return left.formDuplicationWith(right) :: genSVs(num - 1, alus)
          }
        }
      }
      // failed to find a right, try again
      genSVs(num, alus)
    }
  }

  class SequenceVarier(val variations : SortedSet[Variation], val inputChars : Iterator[Char], val out : Writer) {

    def processHeader(inputChars : Iterator[Char]) {
      val c = inputChars.next()
      out.write(c)
      if (c != '\n') {
        processHeader(inputChars)
      }
    }

    def printBases(c : Iterator[Char], printedChars : Int) : Int = {
      var num = 0
      while (c.hasNext) {
        out.write(c.next())
        num = num + 1
        if ((printedChars + num) % 60 == 0) {
          out.write("\n")
        }
      }
      num
    }

    def processBase(c : Char, offset : Int, printedChars : Int, currentOrNext : Variation) : (Int, Int) = {
      var newPrinted = printedChars

      if (currentOrNext != null && currentOrNext.covers(offset)) {
        newPrinted = newPrinted + printBases(currentOrNext.processBaseInside(c), printedChars)
      } else {
        newPrinted = newPrinted + printBases(Iterator(c), printedChars)
      }

      if (currentOrNext != null && currentOrNext.endsAt(offset)) {
        newPrinted = newPrinted + printBases(currentOrNext.processEnd(), newPrinted)
      }
      (offset + 1, newPrinted)
    }

    def processSequence() {
      val variationIterator = variations.iterator
      var offset = 1
      var printedChars = 0
      var currentOrNextVariation = variationIterator.next()
      for (c <- inputChars) {
        if (c == '>') {
          out.write(c)
          processHeader(inputChars)
        } else {
          if (c != '\n' && c != '\r') {
            val result = processBase(c, offset, printedChars, currentOrNextVariation)
            if (currentOrNextVariation != null && currentOrNextVariation.endsAt(offset)) {
              if (variationIterator.hasNext) {
                currentOrNextVariation = variationIterator.next()
              } else {
                currentOrNextVariation = null
              }
            }

            offset = result._1
            printedChars = result._2
          }
        }
      }
      out.write("\n")
      println(offset)
      println(printedChars)

    }
  }

  /**
   * Parses the GFF files from the Venter genome Homozygous indel files:
   * chrom  id  "homozygous_indel"  start  end  .  +  .  .  sequence  "Homozygous_Deletion|Homozygous_Insertion"
   * @param filename GFF annotation file
   * @return
   */
  def parseGffFile(filename : String) : List[Variation] = {
    var variations = new scala.collection.mutable.ListBuffer[Variation]
    var chrom : String = null
    for(line <- Source.fromFile(filename).getLines()) {
      val fields = line.split("\t")
      // gff is one-based
      if (fields.length < 10) {
        println("could not parse line: " + line)
      }
      if (chrom == null) {
        chrom = fields(0)
      } else {
        if (chrom != fields(0)) {
          throw new IllegalArgumentException("Found a new chromosome value: " + fields(0) + " when we had been processing " + chrom + ". This script currently only processes single chromsosome GFFs")
        }
      }

      val start = fields(3).toInt
      val end = fields(4).toInt
      val varType = fields(10)
      if ("Homozygous_Deletion".equals(varType)) {
        variations += new Deletion(start, end)
      } else if ("Homozygous_Insertion".equals(varType)) {
        variations += new Insertion(start, fields(9))
      }

    }
    variations.toList
  }


  def main(args: Array[String]) {
    if (args.length == 3) {

      val gffFileName = args(0)
      val chrFastaFile = args(1)

      val variations = SortedSet(parseGffFile(gffFileName) : _*)(Ordering[Int].on[Variation](_ begin))

      val out = new BufferedWriter(new FileWriter(args(2)))

      val inputChars = Source.fromFile(chrFastaFile)

      try {
        val s = new SequenceVarier(variations, inputChars, out)
        s.processSequence()
      } finally {
        out.flush()
        out.close()
      }
    } else {
      println("usage: applyVariantsToFasta.scala gffFile ref newref")
    }
  }
}

}
