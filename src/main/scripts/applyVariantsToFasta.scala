import scala.io.Source
import scala.util.Random
import scala.collection.mutable.ListBuffer

import java.io._

def printToFile(f: java.io.File)(op: java.io.PrintWriter => Unit) {
  val p = new java.io.PrintWriter(f)
  try { op(p) } finally { p.close() }
}

abstract class Variation(begin : Int, end : Int) {
  def covers(o : Int) : Boolean = 
    o >= begin && o < end
  def endsAt(o : Int) : Boolean = 
    o == end - 1

  def processBaseInside(c : Char) : Iterator[Char]
  def processEnd() : Iterator[Char]
}

class Deletion(begin : Int, end : Int) extends Variation(begin, end) {
  def processBaseInside(c : Char) : Iterator[Char] = Iterator()
  def processEnd() : Iterator[Char] = Iterator()

  override def toString() = "2\t" + begin + "\t" + end + "\t" + "Deletion:" + begin + "-" + end + "\t" + (end - begin)
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
    var printed = 0
    // for (c <- basesSpanned) {
    //   printed = printed + printBase(c)
    // }
    basesSpanned.iterator
  }
  override def toString() = "2\t" + begin + "\t" + end + "\t" + "Inversion:" + begin + "-" + end + "\t" + (end - begin)
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

  override def toString() = "2\t" + begin + "\t" + end + "\t" + "Duplication:" + begin + "-" + end + "\t" + (end - begin)
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
    return genSVs(num, alus)      
  }  
}

class SequenceVarier(val variations : List[Variation], val inputChars : Iterator[Char], val out : Writer) {

  def processHeader(inputChars : Iterator[Char]) : Unit = {
    val c = inputChars.next
    out.write(c)
    if (c != '\n') {
      processHeader(inputChars)
    }
  }
  
  def printBases(c : Iterator[Char], printedChars : Int) : Int = {
    var num = 0
    while (c.hasNext) {
      out.write(c.next)
      num = num + 1
      if ((printedChars + num) % 60 == 0) {
        out.write("\n")
      }
    }
    num
  }
  
  def processBase(c : Char, offset : Int, printedChars : Int, variations : List[Variation]) : (Int, Int) = {
    var printed = false
    var newPrinted = printedChars
    
    val currentVariations = variations.filter(v => v.covers(offset))
    assert(currentVariations.length == 0 || currentVariations.length == 1)

    if (currentVariations.length == 1) {
      newPrinted = newPrinted + printBases(currentVariations(0).processBaseInside(c), printedChars)
    } else {
      newPrinted = newPrinted + printBases(Iterator(c), printedChars)
    }
  
    val endingVariations = variations.filter(v => v.endsAt(offset))
    assert(endingVariations.length == 0 || endingVariations.length == 1)
    if (endingVariations.length == 1) {
      newPrinted = newPrinted + printBases(endingVariations(0).processEnd(), newPrinted)
    }
    (offset + 1, newPrinted)
  }
  
  def processSequence() : Unit = {
    val line_width = 60
    var offset = 0
    var printedChars = 0
    for (c <- inputChars) {
      if (c == '>') {
        out.write(c)
        processHeader(inputChars)
      } else {
        if (c != '\n' && c != '\r') {
          val result = processBase(c, offset, printedChars, variations)
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

def parseGffFile(filename : String) : List[Variation] = {
  var variations = new scala.collection.mutable.ListBuffer[Variation]
  for(line <- Source.fromFile(filename).getLines()) {
    val fields = line.split("\t")
    // gff is one-based
    val start = fields(3).toInt - 1
    val end = fields(4).toInt - 1
    variations += new Deletion(start, end)
  }
  return variations.toList
}

if (args.length == 3) {

  val gffFileName = args(0)
  val chrFastaFile = args(1)

  val variations = parseGffFile(gffFileName)

  val out = new FileWriter(args(2))
  
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

