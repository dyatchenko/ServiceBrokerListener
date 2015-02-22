
namespace ServiceBrokerListener.WebUI.Infrastructure
{
    using System.Web.Mvc;
    using System;
    using System.Collections.Generic;

    using Moq;
    using Ninject;
    using ServiceBrokerListener.WebUI.Abstract;

    using TableRow = ServiceBrokerListener.WebUI.Models.TableRow;

    public class NinjectDependencyResolver : IDependencyResolver
    {
        private readonly IKernel kernel;

        public NinjectDependencyResolver(IKernel kernelParam)
        {
            kernel = kernelParam;
            AddBindings();
        }

        public object GetService(Type serviceType)
        {
            return kernel.TryGet(serviceType);
        }

        public IEnumerable<object> GetServices(Type serviceType)
        {
            return kernel.GetAll(serviceType);
        }

        private void AddBindings()
        {
            var list = new List<TableRow>
                    {
                        new TableRow {A = "Year", B = "Maserati", C = "Mazda", D = "Mercedes", E = "Mini", F = "Mitsubishi"},
                        new TableRow {A = "2009", B = "0", C = "2941", D = "4303", E = "354", F = "5814"}
                    };
            for (int i = 0; i < 12; i++)
                list.Add(new TableRow { A = "2009", B = "0", C = "2941", D = "4303", E = "354", F = "6456645" });

            Mock<ITableRowRepository> mock = new Mock<ITableRowRepository>();
            mock.Setup(m => m.Rows).Returns(list);

            mock.Setup(m => m.UpdateRow(It.IsAny<int>(), It.IsAny<int>(), It.IsAny<string>()))
                .Callback<int, int, string>(
                    (r, c, s) =>
                        {
                            if (r < 0 || r >= list.Count || c < 0 || c >= 6) return;

                            switch (c)
                            {
                                case 0: list[r].A = s; break;
                                case 1: list[r].B = s; break;
                                case 2: list[r].C = s; break;
                                case 3: list[r].D = s; break;
                                case 4: list[r].E = s; break;
                                case 5: list[r].F = s; break;
                            }

                            mock.Raise(
                                mk => mk.TableChanged += null,
                                new TableChangedEventArgs(
                                    new[]
                                        {
                                            new TableChangedEventArgs.SingleChange
                                                {
                                                    Column = c,
                                                    Row = r,
                                                    NewValue = s
                                                }
                                        }));
                        });
            kernel.Bind<ITableRowRepository>().ToConstant(mock.Object);
        }
    }
}