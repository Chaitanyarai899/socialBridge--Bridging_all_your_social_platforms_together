import { Link } from "react-router-dom";
import { useUser } from '@clerk/clerk-react';

const Stats = () => {

  const { isSignedIn, user} = useUser();

  return (
    <div>
      {" "}
      <div className="relative isolate bg-gray-800 -mt-32 ">
        <div
          className="absolute inset-x-0 -top-40 -z-10 transform-gpu overflow-hidden blur-3xl sm:-top-80"
          aria-hidden="true"
        >
          <div
            className="relative left-[calc(50%-11rem)] aspect-[1155/678] w-[36.125rem] -translate-x-1/2 rotate-[30deg] bg-gradient-to-tr from-[#ff80b5] to-[#9089fc] opacity-20 sm:left-[calc(50%-30rem)] sm:w-[72.1875rem]"
            style={{
              clipPath:
                "polygon(74.1% 44.1%, 100% 61.6%, 97.5% 26.9%, 85.5% 0.1%, 80.7% 2%, 72.5% 32.5%, 60.2% 62.4%, 52.4% 68.1%, 47.5% 58.3%, 45.2% 34.5%, 27.5% 76.7%, 0.1% 64.9%, 17.9% 100%, 27.6% 76.8%, 76.1% 97.7%, 74.1% 44.1%)",
            }}
          />
        </div>
        <div className="py-24 sm:py-32 lg:pb-40">
          <div className="mx-auto max-w-7xl px-6 lg:px-8">
            <div className="mx-auto max-w-2xl text-center">
              <h1 className="text-4xl font-bold tracking-tight text-white sm:text-6xl">
               Your Stats:
              </h1>
              <div className="mt-10 flex items-center justify-center gap-x-6">
              {isSignedIn ? (
                <>
                 <h1 className="text-4xl font-bold tracking-tight text-white sm:text-2xl">welcome {user.firstName}!</h1>
                 <Link to="/home" className="btn-main">
                  View Your Stats
               </Link>
               </>
                 ) : (
                  <Link to="/home" className="btn-main">
                  Create an Account
                </Link>
                  )} 
              </div>
            </div>
         </div>
       </div>
     </div>
    </div>
  );
};

export default Stats;