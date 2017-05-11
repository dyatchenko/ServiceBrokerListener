namespace ServiceBrokerListener.WebUI.Infrastructure
{
    using Microsoft.AspNet.SignalR;

    using ServiceBrokerListener.WebUI.Abstract;

    public class HomeHub : Hub
    {
        public ITableRowRepository Repository { get; internal set; }

        public void Send(string message)
        {
            // Call the broadcastMessage method to update clients.
            Clients.Others.broadcastMessage(message);
        }
    }
}