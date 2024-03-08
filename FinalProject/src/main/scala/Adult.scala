case class Adult(age: Int, workclass: String, fnlwgt: Int,
                 education: String, educationNum: Int, maritalStatus: String,
                 occupation: String, relationship: String, race: String, sex: String,
                 capitalGain: Int, capitalLoss: Int, hoursPerWeek: Int,
                 nativeCountry: String, income: String)
{
  def getFeatures: List[String] = {
    List("age","workclass","fnlwgt","education","educationNum",
      "maritalStatus","occupation","relationship","race","sex","capitalGain",
      "capitalLoss","hoursPerWeek","nativeCountry","income"
    )
  }

  def getLabel: String = {
    this.income
  }

  def getFeatureAsString(featureName: String): String = {
    if (featureName.equals("age")) {this.age.toString}
    else if (featureName.equals("workclass")) {this.workclass}
    else if (featureName.equals("fnlwgt")) {this.fnlwgt.toString}
    else if (featureName.equals("education")) {this.education}
    else if (featureName.equals("educationNum")) {this.educationNum.toString}
    else if (featureName.equals("maritalStatus")) {this.maritalStatus}
    else if (featureName.equals("occupation")) {this.occupation}
    else if (featureName.equals("relationship")) {this.relationship}
    else if (featureName.equals("race")) {this.race}
    else if (featureName.equals("sex")) {this.sex}
    else if (featureName.equals("capitalGain")) {this.capitalGain.toString}
    else if (featureName.equals("capitalLoss")) {this.capitalLoss.toString}
    else if (featureName.equals("hoursPerWeek")) {this.hoursPerWeek.toString}
    else if (featureName.equals("nativeCountry")) {this.nativeCountry}
    else if (featureName.equals("income")) {this.income}
    else {throw new IllegalArgumentException("Feature not found " + featureName)}
  }
}
