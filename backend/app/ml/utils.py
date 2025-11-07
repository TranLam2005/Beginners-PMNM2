def evalute_attp_model(predict_rate):
  if predict_rate >= 0.8:
    return "Xuất sắc"
  elif predict_rate >= 0.6:
    return "Tốt"
  elif predict_rate >= 0.4:
    return "Trung bình"
  return "Cần cải thiện"  