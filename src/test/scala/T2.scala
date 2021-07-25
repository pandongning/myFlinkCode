object T2 {
  def main(args: Array[String]): Unit = {
    val student = new Student("pdn", 23, "sex")
    println(student.myName + "\t" + student.age)
    sum2(1)(2)
  }

  def sum2(a: Int) = {(b: Int) => a + b}
}


class Student(sex: String) {
  var myName: String = ""
  var age: Int = 0

  def this(myName: String, age: Int, sex: String) {
    this(sex)
    this.myName = myName
    this.age = age
  }

}
