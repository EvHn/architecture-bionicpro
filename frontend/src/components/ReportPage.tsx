import React, { useState } from 'react';

const ReportPage: React.FC = () => {
  const [loading, setLoading] = useState(false);
  const [auth, setAuth] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const downloadReport = async () => {
    try {
      setLoading(true);
      setError(null);

      const response = await fetch(`${process.env.REACT_APP_API_URL}/reports`, {
        credentials: 'include'
      });

    } catch (err) {
      setError(err instanceof Error ? err.message : 'An error occurred');
    } finally {
      setLoading(false);
    }
  };

  fetch(`${process.env.REACT_APP_BACK_URL}/api/me`, { credentials: 'include' })
    .then(res => {
      if (res.status === 401) {
        window.location.href = `${process.env.REACT_APP_BACK_URL}/oauth2/authorization/keycloak`;
        return;
      }
      setAuth(true);
      return res.json();
    });

  return (auth ?
    (<div className="flex flex-col items-center justify-center min-h-screen bg-gray-100">
      <div className="p-8 bg-white rounded-lg shadow-md">
        <h1 className="text-2xl font-bold mb-6">Usage Reports</h1>
        <button
          onClick={downloadReport}
          disabled={loading}
          className={`px-4 py-2 bg-blue-500 text-white rounded hover:bg-blue-600 ${loading ? 'opacity-50 cursor-not-allowed' : ''
            }`}
        >
          {loading ? 'Generating Report...' : 'Download Report'}
        </button>

        {error && (
          <div className="mt-4 p-4 bg-red-100 text-red-700 rounded">
            {error}
          </div>
        )}
      </div>
    </div>) : <></>
  );
};

export default ReportPage;