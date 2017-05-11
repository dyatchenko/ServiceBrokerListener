using System.IO;
using System.Xml;
using System;
using System.Xml.Linq;

namespace ServiceBrokerListener.Domain
{
		public class TableChangedEventArgs : EventArgs
		{
			private readonly string notificationMessage;

			private static class Tags
			{
				public const string Inserted = @"inserted";

				public const string Deleted = @"deleted";
			}


			public TableChangedEventArgs(string notificationMessage)
			{
				this.notificationMessage = notificationMessage;
			}

			public XElement Data => string.IsNullOrWhiteSpace(notificationMessage)
				? null
				: ReadXDocumentWithInvalidCharacters(notificationMessage);

			public NotificationTypes NotificationType => Data?.Element(Tags.Inserted) != null
				? Data?.Element(Tags.Deleted) != null
					? NotificationTypes.Update
					: NotificationTypes.Insert
				: Data?.Element(Tags.Deleted) != null
					? NotificationTypes.Delete
					: NotificationTypes.None;

			/// <summary>
			/// Converts an xml string into XElement with no invalid characters check.
			/// https://paulselles.wordpress.com/2013/07/03/parsing-xml-with-invalid-characters-in-c-2/
			/// </summary>
			/// <param name="xml">The input string.</param>
			/// <returns>The result XElement.</returns>
			private static XElement ReadXDocumentWithInvalidCharacters(string xml)
			{
				XDocument xDocument;

				var xmlReaderSettings = new XmlReaderSettings {CheckCharacters = false};

				using (var stream = new StringReader(xml))
				using (var xmlReader = XmlReader.Create(stream, xmlReaderSettings))
				{
					// Load our XDocument
					xmlReader.MoveToContent();
					xDocument = XDocument.Load(xmlReader);
				}

				return xDocument.Root;
			}
		}
	}