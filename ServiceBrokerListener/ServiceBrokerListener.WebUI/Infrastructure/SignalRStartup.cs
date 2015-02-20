using Microsoft.Owin;

using ServiceBrokerListener.WebUI.Infrastructure;

[assembly: OwinStartup(typeof(SignalRStartup))]
namespace ServiceBrokerListener.WebUI.Infrastructure
{
    using Owin;

    public class SignalRStartup
    {
        public void Configuration(IAppBuilder app)
        {
            // Any connection or hub wire up and configuration should go here
            app.MapSignalR();
        }
    }
}