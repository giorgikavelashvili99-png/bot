const { io } = require("socket.io-client");
const customer = io("http://localhost:3000");
const adminSock = io("http://localhost:3000");
let received = false;

adminSock.on("connect", () => adminSock.emit("join_admin"));
adminSock.on("admin_notify", (data) => {
  console.log("[admin] received admin_notify:", JSON.stringify(data));
  received = true;
});

customer.on("connect", () => {
  customer.emit("join_order", { orderId: "fcmtest2" });
  setTimeout(() => {
    customer.emit("send_message", { orderId: "fcmtest2", message: { from: "customer", text: "does this still work?" } });
  }, 300);
});

setTimeout(() => {
  console.log(received ? "TEST PASSED -- send_message with FCM hook intact still works" : "TEST FAILED");
  process.exit(received ? 0 : 1);
}, 1200);
