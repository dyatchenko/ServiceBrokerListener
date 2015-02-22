namespace ServiceBrokerListener.WebUI.Controllers
{
    using Microsoft.AspNet.SignalR;

    using ServiceBrokerListener.WebUI.Abstract;
    using System.Web.Mvc;

    using ServiceBrokerListener.WebUI.Infrastructure;

    public class HomeController : Controller
    {
        private readonly ITableRowRepository repo;

        static HomeController()
        {
            var repo = (ITableRowRepository)DependencyResolver.Current.GetService(typeof(ITableRowRepository));
            repo.TableChanged += RepoTableChanged;
        }

        public HomeController(ITableRowRepository repo)
        {
            this.repo = repo;
        }

        [HttpGet]
        public ViewResult Index()
        {
            return View(repo.Rows);
        }

        [HttpPost]
        [ValidateInput(false)]
        public string SetTableChanges(string json)
        {
            const string CoordidatesPropertyName = "0";

            if (string.IsNullOrWhiteSpace(json)) return "EMPTY";

            dynamic obj = System.Web.Helpers.Json.Decode(json);

            if (obj[CoordidatesPropertyName] == null) return "EMPTY";

            var arr = obj[CoordidatesPropertyName];
            for (int i = 0; i < arr.Length; i++)
            {
                int row = arr[i][0];
                int col = arr[i][1];
                string value = arr[i][3];

                repo.UpdateRow(row, col, value);
            }

            return "OK";
        }

        private static void RepoTableChanged(object sender, TableChangedEventArgs e)
        {
            var context = GlobalHost.ConnectionManager.GetHubContext<HomeHub>();
            context.Clients.All.broadcastMessage(
                string.Format("{{\"0\": {0}}}", e.GetChangesAsJson()));
        }
    }
}
