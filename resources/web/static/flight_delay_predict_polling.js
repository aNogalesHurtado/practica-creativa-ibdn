var socket = io();
var pendingUUID = null;

$("#flight_delay_classification").submit(function(event) {
  event.preventDefault();
  var url = $(this).attr("action");
  var posting = $.post(url, $("#flight_delay_classification").serialize());
  posting.done(function(data) {
    var response = JSON.parse(data);
    if (response.status == "OK") {
      pendingUUID = response.id;
      $("#result").empty().append("Processing...");
    }
  });
});

socket.on('prediction', function(prediction) {
  if (pendingUUID && prediction.UUID === pendingUUID) {
    renderPage(prediction);
    pendingUUID = null;
  }
});

function renderPage(response) {
  console.log(response);
  var displayMessage;
  if (response.Prediction == 0 || response.Prediction == '0') {
    displayMessage = "Early (15+ Minutes Early)";
  } else if (response.Prediction == 1 || response.Prediction == '1') {
    displayMessage = "Slightly Early (0-15 Minute Early)";
  } else if (response.Prediction == 2 || response.Prediction == '2') {
    displayMessage = "Slightly Late (0-30 Minute Delay)";
  } else if (response.Prediction == 3 || response.Prediction == '3') {
    displayMessage = "Very Late (30+ Minutes Late)";
  }
  console.log(displayMessage);
  $("#result").empty().append(displayMessage);
}
